/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! SCP uploads over an authenticated SSH session.

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use super::client::{SSH_UPLOAD_BUFFER_SIZE_BYTES, SshClient, SshHandler, SshUploadOptions};
use super::error::{SshError, SshResult};
use super::exec::ssh_output_context;
use super::request::{ChannelRequestFailureCode, wait_for_channel_request_confirmation};
use crate::utilities::error::{Result, RmsError};

use russh::{Channel, ChannelMsg, client};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

/// File mode sent in the SCP control record for uploaded files.
const SCP_FILE_MODE: &str = "0644";

/// Maximum remote SCP error text retained in a returned error.
const SCP_ERROR_MESSAGE_MAX_BYTES: usize = 4 * 1024;

/// Maximum remote standard-error bytes retained while waiting for the SCP sink.
const SCP_STDERR_MAX_BYTES: usize = 4 * 1024;

/// Immutable parameters shared by the ordered phases of one SCP transfer.
struct ScpTransfer<'a> {
    host: &'a str,
    local_path: &'a str,
    remote_path: &'a str,
    file_name: &'a str,
    file_size_bytes: u64,
    step_timeout: Duration,
    cancel: &'a CancellationToken,
}

impl SshClient {
    /// Upload a local file to the remote host via SCP.
    ///
    /// Uses a fixed 512 KiB buffer and the default step timeout, capped by
    /// `timeout`, with `timeout` as the overall upload timeout. The remote file
    /// requests mode `0644` through the remote `scp -t` sink process; the
    /// remote process may further restrict that mode according to its umask.
    ///
    /// `cancel` interrupts local file setup or an in-flight transfer. If the
    /// channel is already open, cancellation drops it. A partially written
    /// remote file may remain after cancellation or a timeout because the SCP
    /// protocol does not provide transactional writes.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for an invalid path, `FailedPrecondition` if
    /// the client is not connected, `NotFound` if `local_path` cannot be
    /// opened, `Timeout` if the upload or one transfer step times out,
    /// `Cancelled` if `cancel` is triggered, or `Internal`/`Unavailable` for
    /// SSH and SCP protocol failures.
    pub async fn scp_upload(
        &self,
        local_path: &str,
        remote_path: &str,
        timeout: Duration,
        cancel: CancellationToken,
    ) -> Result<()> {
        let default_options = SshUploadOptions::default();

        self.scp_upload_with_options(
            local_path,
            remote_path,
            SshUploadOptions {
                overall_timeout: timeout,
                step_timeout: default_options.step_timeout.min(timeout),
            },
            cancel,
        )
        .await
    }

    /// Upload a local file to the remote host via SCP using explicit tunables.
    ///
    /// The file is streamed from disk and its size is captured before the
    /// transfer because SCP requires the byte count in its control record. If
    /// the file shrinks during upload, the operation fails rather than sending
    /// a shorter payload than the remote sink expects. Growth beyond the
    /// captured size is ignored.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `options` or either path cannot be used for
    /// a single-file SCP transfer, `FailedPrecondition` if the client is not
    /// connected, `NotFound` if `local_path` cannot be opened, `Timeout` if
    /// local file setup, the upload, or one transfer step times out, `Cancelled`
    /// if `cancel` is triggered, or `Internal`/`Unavailable` for SSH and SCP
    /// failures.
    pub async fn scp_upload_with_options(
        &self,
        local_path: &str,
        remote_path: &str,
        options: SshUploadOptions,
        cancel: CancellationToken,
    ) -> Result<()> {
        options.validate()?;
        let handle = self.handle.as_ref().ok_or(SshError::NotConnected)?;

        if remote_path.is_empty()
            || remote_path.contains('\0')
            || remote_path.contains(['\r', '\n'])
        {
            return Err(RmsError::invalid_argument(
                "SCP remote path must be non-empty and contain no NUL or newline characters",
            ));
        }

        let file_name = Path::new(local_path)
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty() && !name.contains('\0') && !name.contains(['\r', '\n']))
            .ok_or_else(|| {
                RmsError::invalid_argument(
                    "SCP local path must end in a filename without NUL or newline characters",
                )
            })?;

        let host = self.host.as_str();
        let timeout_secs = options.overall_timeout.as_secs();
        let step_timeout_secs = options.step_timeout.as_secs();

        tracing::info!(
            host,
            local_path,
            remote_path,
            timeout_secs,
            step_timeout_secs,
            buffer_size_bytes = SSH_UPLOAD_BUFFER_SIZE_BYTES,
            "SCP upload starting"
        );

        let upload = with_scp_timeout_and_cancellation(
            host,
            remote_path,
            "SCP upload",
            options.overall_timeout,
            &cancel,
            async {
                let local_file = with_scp_timeout_and_cancellation(
                    host,
                    local_path,
                    "SCP local file open",
                    options.step_timeout,
                    &cancel,
                    async {
                        tokio::fs::File::open(local_path).await.map_err(|error| {
                            tracing::warn!(
                                host,
                                local_path,
                                error = %error,
                                "SCP local file open failed"
                            );

                            SshError::LocalFileOpenFailed {
                                path: local_path.to_owned(),
                                details: error.to_string(),
                            }
                        })
                    },
                )
                .await?;

                let metadata = with_scp_timeout_and_cancellation(
                    host,
                    local_path,
                    "SCP local file metadata",
                    options.step_timeout,
                    &cancel,
                    async {
                        local_file
                            .metadata()
                            .await
                            .map_err(|error| SshError::ScpOperationFailed {
                                operation: "local file metadata",
                                path: local_path.to_owned(),
                                details: error.to_string(),
                            })
                    },
                )
                .await?;

                if !metadata.is_file() {
                    return Err(RmsError::invalid_argument(format!(
                        "SCP local path is not a regular file: {local_path}"
                    )));
                }

                let file_size_bytes = metadata.len();

                let transfer = ScpTransfer {
                    host,
                    local_path,
                    remote_path,
                    file_name,
                    file_size_bytes,
                    step_timeout: options.step_timeout,
                    cancel: &cancel,
                };

                let bytes_sent = run_scp_upload(handle, local_file, &transfer).await?;

                Ok::<_, RmsError>((bytes_sent, file_size_bytes))
            },
        )
        .await;

        if let Err(error) = &upload {
            tracing::warn!(host, local_path, remote_path, error = %error, "SCP upload failed");
        }

        let (bytes_sent, file_size_bytes) = upload?;

        tracing::info!(
            host,
            local_path,
            remote_path,
            bytes_sent,
            file_size_bytes,
            buffer_size_bytes = SSH_UPLOAD_BUFFER_SIZE_BYTES,
            "SCP upload completed"
        );

        Ok(())
    }
}

/// Run the remote SCP sink and transfer one local file through its SSH channel.
async fn run_scp_upload(
    handle: &client::Handle<SshHandler>,
    mut local_file: tokio::fs::File,
    transfer: &ScpTransfer<'_>,
) -> SshResult<u64> {
    let mut channel = with_scp_timeout_and_cancellation(
        transfer.host,
        transfer.remote_path,
        "SCP channel open",
        transfer.step_timeout,
        transfer.cancel,
        async {
            handle
                .channel_open_session()
                .await
                .map_err(|error| SshError::Unavailable {
                    operation: "SCP setup",
                    details: error.to_string(),
                })
        },
    )
    .await?;

    let command = scp_sink_command(transfer.remote_path);

    with_scp_timeout_and_cancellation(
        transfer.host,
        transfer.remote_path,
        "SCP exec request",
        transfer.step_timeout,
        transfer.cancel,
        async {
            channel
                .exec(true, command)
                .await
                .map_err(|error| SshError::OperationFailed {
                    operation: "SCP exec request",
                    details: error.to_string(),
                })
        },
    )
    .await?;

    let deferred_messages = with_scp_timeout_and_cancellation(
        transfer.host,
        transfer.remote_path,
        "SCP exec confirmation",
        transfer.step_timeout,
        transfer.cancel,
        wait_for_channel_request_confirmation(
            &mut channel,
            "SCP exec request",
            ChannelRequestFailureCode::OperationFailed,
        ),
    )
    .await?;

    with_scp_timeout_and_cancellation(
        transfer.host,
        transfer.remote_path,
        "SCP initial response",
        transfer.step_timeout,
        transfer.cancel,
        wait_for_scp_initial_response(&mut channel, transfer.remote_path, deferred_messages),
    )
    .await?;

    let mut stream = channel.into_stream();

    send_scp_file(&mut local_file, &mut stream, transfer).await
}

/// Wait for the remote sink's initial ACK while retaining SSH diagnostics.
async fn wait_for_scp_initial_response(
    channel: &mut Channel<client::Msg>,
    remote_path: &str,
    deferred_messages: Vec<ChannelMsg>,
) -> SshResult<()> {
    let mut deferred_messages = deferred_messages.into_iter();
    let mut response = Vec::new();
    let mut stderr = Vec::new();
    let mut exit_status = None;

    loop {
        let message = if let Some(message) = deferred_messages.next() {
            Some(message)
        } else {
            channel.wait().await
        };

        let Some(message) = message else {
            return Err(SshError::ScpOperationFailed {
                operation: "initial response",
                path: remote_path.to_owned(),
                details: scp_channel_ended_details(
                    "channel closed before the initial SCP response",
                    exit_status,
                    &stderr,
                ),
            });
        };

        if record_scp_initial_message(
            &mut response,
            &mut stderr,
            &mut exit_status,
            remote_path,
            message,
        )? {
            return Ok(());
        }
    }
}

/// Record one SSH channel message while waiting for the initial SCP ACK.
fn record_scp_initial_message(
    response: &mut Vec<u8>,
    stderr: &mut Vec<u8>,
    exit_status: &mut Option<u32>,
    remote_path: &str,
    message: ChannelMsg,
) -> SshResult<bool> {
    match message {
        ChannelMsg::Data { data } => {
            response.extend_from_slice(&data);
            parse_scp_response(response, remote_path, "initial response")
        }
        ChannelMsg::ExtendedData { data, .. } => {
            let remaining = SCP_STDERR_MAX_BYTES.saturating_sub(stderr.len());
            stderr.extend(data.iter().copied().take(remaining));

            Ok(false)
        }
        ChannelMsg::ExitStatus {
            exit_status: status,
        } => {
            *exit_status = Some(status);

            Ok(false)
        }
        ChannelMsg::ExitSignal {
            signal_name,
            error_message,
            ..
        } => Err(SshError::ScpOperationFailed {
            operation: "initial response",
            path: remote_path.to_owned(),
            details: scp_channel_ended_details(
                &format!(
                    "remote SCP process terminated by signal {signal_name:?}: {error_message}"
                ),
                *exit_status,
                stderr,
            ),
        }),
        ChannelMsg::Eof | ChannelMsg::Close => Err(SshError::ScpOperationFailed {
            operation: "initial response",
            path: remote_path.to_owned(),
            details: scp_channel_ended_details(
                "channel closed before the initial SCP response",
                *exit_status,
                stderr,
            ),
        }),
        ChannelMsg::WindowAdjusted { .. } => Ok(false),
        unexpected => Err(SshError::ScpOperationFailed {
            operation: "initial response",
            path: remote_path.to_owned(),
            details: format!(
                "unexpected channel message while waiting for the initial SCP response: {unexpected:?}"
            ),
        }),
    }
}

/// Add bounded, redacted remote process context to an early channel failure.
fn scp_channel_ended_details(reason: &str, exit_status: Option<u32>, stderr: &[u8]) -> String {
    let exit_context = exit_status
        .map(|status| format!("; exit status {status}"))
        .unwrap_or_default();

    let output_context = ssh_output_context(&[], stderr);

    format!("{reason}{exit_context}{output_context}")
}

/// Perform the remaining acknowledgement-driven protocol for one regular file.
async fn send_scp_file<R, S>(
    local_file: &mut R,
    stream: &mut S,
    transfer: &ScpTransfer<'_>,
) -> SshResult<u64>
where
    R: AsyncRead + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_scp_control_record(stream, transfer).await?;
    let bytes_sent = write_scp_payload(local_file, stream, transfer).await?;
    finish_scp_file(stream, transfer).await?;

    Ok(bytes_sent)
}

/// Send the SCP file control record and wait for the sink to accept it.
async fn write_scp_control_record<S>(stream: &mut S, transfer: &ScpTransfer<'_>) -> SshResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let header = format!(
        "C{SCP_FILE_MODE} {} {}\n",
        transfer.file_size_bytes, transfer.file_name
    );

    with_scp_timeout_and_cancellation(
        transfer.host,
        transfer.remote_path,
        "SCP control record write",
        transfer.step_timeout,
        transfer.cancel,
        async {
            stream.write_all(header.as_bytes()).await.map_err(|error| {
                SshError::ScpOperationFailed {
                    operation: "control record write",
                    path: transfer.remote_path.to_owned(),
                    details: error.to_string(),
                }
            })?;

            stream
                .flush()
                .await
                .map_err(|error| SshError::ScpOperationFailed {
                    operation: "control record flush",
                    path: transfer.remote_path.to_owned(),
                    details: error.to_string(),
                })
        },
    )
    .await?;

    with_scp_timeout_and_cancellation(
        transfer.host,
        transfer.remote_path,
        "SCP control response",
        transfer.step_timeout,
        transfer.cancel,
        read_scp_response(stream, transfer.remote_path, "control response"),
    )
    .await
}

/// Stream exactly the byte count advertised in the SCP control record.
async fn write_scp_payload<R, S>(
    local_file: &mut R,
    stream: &mut S,
    transfer: &ScpTransfer<'_>,
) -> SshResult<u64>
where
    R: AsyncRead + Unpin,
    S: AsyncWrite + Unpin,
{
    let mut buffer = vec![0_u8; SSH_UPLOAD_BUFFER_SIZE_BYTES];
    let mut remaining = transfer.file_size_bytes;
    let mut bytes_sent = 0_u64;

    while remaining != 0 {
        let read_capacity = remaining.min(buffer.len() as u64) as usize;

        let chunk_buffer =
            buffer
                .get_mut(..read_capacity)
                .ok_or_else(|| SshError::ScpOperationFailed {
                    operation: "local file read",
                    path: transfer.local_path.to_owned(),
                    details: "calculated read size exceeded the transfer buffer".to_owned(),
                })?;

        let bytes_read = with_scp_timeout_and_cancellation(
            transfer.host,
            transfer.local_path,
            "SCP local file read",
            transfer.step_timeout,
            transfer.cancel,
            async {
                local_file
                    .read(chunk_buffer)
                    .await
                    .map_err(|error| SshError::ScpOperationFailed {
                        operation: "local file read",
                        path: transfer.local_path.to_owned(),
                        details: error.to_string(),
                    })
            },
        )
        .await?;

        if bytes_read == 0 {
            return Err(SshError::ScpOperationFailed {
                operation: "local file read",
                path: transfer.local_path.to_owned(),
                details: format!(
                    "file ended after {bytes_sent} bytes; control record declared {}",
                    transfer.file_size_bytes
                ),
            });
        }

        let chunk = buffer
            .get(..bytes_read)
            .ok_or_else(|| SshError::ScpOperationFailed {
                operation: "local file read",
                path: transfer.local_path.to_owned(),
                details: format!(
                    "read {bytes_read} bytes into a {} byte transfer buffer",
                    buffer.len()
                ),
            })?;

        with_scp_timeout_and_cancellation(
            transfer.host,
            transfer.remote_path,
            "SCP file data write",
            transfer.step_timeout,
            transfer.cancel,
            async {
                stream
                    .write_all(chunk)
                    .await
                    .map_err(|error| SshError::ScpOperationFailed {
                        operation: "file data write",
                        path: transfer.remote_path.to_owned(),
                        details: error.to_string(),
                    })
            },
        )
        .await?;

        let written = bytes_read as u64;
        remaining -= written;
        bytes_sent += written;
    }

    Ok(bytes_sent)
}

/// Terminate the file payload, verify its final ACK, and close channel input.
async fn finish_scp_file<S>(stream: &mut S, transfer: &ScpTransfer<'_>) -> SshResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    with_scp_timeout_and_cancellation(
        transfer.host,
        transfer.remote_path,
        "SCP file terminator write",
        transfer.step_timeout,
        transfer.cancel,
        async {
            stream
                .write_all(&[0])
                .await
                .map_err(|error| SshError::ScpOperationFailed {
                    operation: "file terminator write",
                    path: transfer.remote_path.to_owned(),
                    details: error.to_string(),
                })?;

            stream
                .flush()
                .await
                .map_err(|error| SshError::ScpOperationFailed {
                    operation: "file terminator flush",
                    path: transfer.remote_path.to_owned(),
                    details: error.to_string(),
                })
        },
    )
    .await?;

    with_scp_timeout_and_cancellation(
        transfer.host,
        transfer.remote_path,
        "SCP final response",
        transfer.step_timeout,
        transfer.cancel,
        read_scp_response(stream, transfer.remote_path, "final response"),
    )
    .await?;

    with_scp_timeout_and_cancellation(
        transfer.host,
        transfer.remote_path,
        "SCP channel shutdown",
        transfer.step_timeout,
        transfer.cancel,
        async {
            stream
                .shutdown()
                .await
                .map_err(|error| SshError::ScpOperationFailed {
                    operation: "channel shutdown",
                    path: transfer.remote_path.to_owned(),
                    details: error.to_string(),
                })
        },
    )
    .await
}

/// Read one success or error response from the remote SCP sink.
async fn read_scp_response<R>(
    reader: &mut R,
    remote_path: &str,
    operation: &'static str,
) -> SshResult<()>
where
    R: AsyncRead + Unpin,
{
    let mut response = Vec::new();

    loop {
        let mut byte = [0_u8; 1];

        reader
            .read_exact(&mut byte)
            .await
            .map_err(|error| SshError::ScpOperationFailed {
                operation,
                path: remote_path.to_owned(),
                details: format!("could not read SCP response: {error}"),
            })?;

        response.extend_from_slice(&byte);

        if parse_scp_response(&response, remote_path, operation)? {
            return Ok(());
        }
    }
}

/// Parse a complete or partial response from an SCP sink.
///
/// Returns `true` for a complete success response and `false` while a bounded
/// error response still needs its terminating newline.
fn parse_scp_response(
    response: &[u8],
    remote_path: &str,
    operation: &'static str,
) -> SshResult<bool> {
    let Some(status) = response.first().copied() else {
        return Ok(false);
    };

    match status {
        0 if response.len() == 1 => Ok(true),
        0 => Err(SshError::ScpOperationFailed {
            operation,
            path: remote_path.to_owned(),
            details: "remote sent data after a successful SCP response".to_owned(),
        }),
        1 | 2 => {
            let Some(newline_index) = response.iter().position(|byte| *byte == b'\n') else {
                if response.len() <= SCP_ERROR_MESSAGE_MAX_BYTES + 1 {
                    return Ok(false);
                }

                return Err(SshError::ScpOperationFailed {
                    operation,
                    path: remote_path.to_owned(),
                    details: format!(
                        "remote SCP error exceeded {SCP_ERROR_MESSAGE_MAX_BYTES} bytes"
                    ),
                });
            };

            let Some(message) = response.get(1..newline_index) else {
                return Err(SshError::ScpOperationFailed {
                    operation,
                    path: remote_path.to_owned(),
                    details: "remote returned a malformed SCP error response".to_owned(),
                });
            };

            let severity = if status == 1 { "recoverable" } else { "fatal" };
            let message = String::from_utf8_lossy(message);

            Err(SshError::ScpOperationFailed {
                operation,
                path: remote_path.to_owned(),
                details: format!("remote reported {severity} error: {message}"),
            })
        }
        unexpected => Err(SshError::ScpOperationFailed {
            operation,
            path: remote_path.to_owned(),
            details: format!("remote returned invalid SCP response byte {unexpected}"),
        }),
    }
}

/// Quote an SCP sink target as one literal POSIX shell argument.
fn scp_sink_command(remote_path: &str) -> String {
    // A leading dash would otherwise be parsed as an option by the remote SCP
    // process. `./` preserves the relative target while ending option parsing.
    let remote_path = if remote_path.starts_with('-') {
        format!("./{remote_path}")
    } else {
        remote_path.to_owned()
    };

    let quoted_path = remote_path.replace('\'', "'\"'\"'");

    format!("scp -t '{quoted_path}'")
}

/// Apply one timeout to an SCP step and race it against upload cancellation.
async fn with_scp_timeout_and_cancellation<T, E>(
    host: &str,
    path: &str,
    operation: &'static str,
    timeout: Duration,
    cancel: &CancellationToken,
    future: impl Future<Output = std::result::Result<T, E>>,
) -> std::result::Result<T, E>
where
    E: From<SshError>,
{
    // Losing a partially completed read or write aborts the whole SCP channel;
    // this transfer is never resumed from an unknown protocol position.
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            tracing::info!(host, path, operation, "SCP operation cancelled");

            Err(E::from(SshError::Cancelled {
                operation,
                host: host.to_owned(),
            }))
        }
        result = tokio::time::timeout(timeout, future) => {
            result.map_err(|_| {
                tracing::warn!(
                    host,
                    path,
                    operation,
                    timeout_secs = timeout.as_secs(),
                    "SCP operation timed out"
                );

                E::from(SshError::timeout(operation, host, timeout))
            })?
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future;

    use super::*;

    #[test]
    fn scp_sink_command_quotes_shell_metacharacters_and_leading_dash() {
        let quoted = scp_sink_command("-release's image; reboot");

        assert_eq!(quoted, "scp -t './-release'\"'\"'s image; reboot'");
    }

    #[tokio::test]
    async fn send_scp_file_writes_control_record_payload_and_terminator()
    -> std::result::Result<(), Box<dyn Error>> {
        let payload = b"firmware payload";
        let expected_header = format!("C{SCP_FILE_MODE} {} firmware.bin\n", payload.len());
        let (mut client_stream, mut server_stream) = tokio::io::duplex(1024);
        let cancel = CancellationToken::new();
        let mut local_file = payload.as_slice();

        let transfer = ScpTransfer {
            host: "10.0.0.1",
            local_path: "/tmp/firmware.bin",
            remote_path: "/remote/firmware.bin",
            file_name: "firmware.bin",
            file_size_bytes: payload.len() as u64,
            step_timeout: Duration::from_secs(1),
            cancel: &cancel,
        };

        let client = send_scp_file(&mut local_file, &mut client_stream, &transfer);

        let server = async {
            let mut header = vec![0_u8; expected_header.len()];
            server_stream.read_exact(&mut header).await?;
            server_stream.write_all(&[0]).await?;

            let mut received_payload = vec![0_u8; payload.len()];
            server_stream.read_exact(&mut received_payload).await?;

            let mut terminator = [1_u8; 1];
            server_stream.read_exact(&mut terminator).await?;
            server_stream.write_all(&[0]).await?;

            Ok::<_, std::io::Error>((header, received_payload, terminator))
        };

        let (bytes_sent, received) = tokio::join!(client, server);
        let (header, received_payload, terminator) = received?;

        assert_eq!(bytes_sent?, payload.len() as u64);
        assert_eq!(header, expected_header.as_bytes());
        assert_eq!(received_payload, payload);
        assert_eq!(terminator, [0]);

        Ok(())
    }

    #[tokio::test]
    async fn send_scp_file_rejects_source_shorter_than_control_record()
    -> std::result::Result<(), Box<dyn Error>> {
        let payload = b"short";
        let declared_size = payload.len() as u64 + 1;
        let expected_header = format!("C{SCP_FILE_MODE} {declared_size} firmware.bin\n");
        let (mut client_stream, mut server_stream) = tokio::io::duplex(1024);
        let cancel = CancellationToken::new();
        let mut local_file = payload.as_slice();

        let transfer = ScpTransfer {
            host: "10.0.0.1",
            local_path: "/tmp/firmware.bin",
            remote_path: "/remote/firmware.bin",
            file_name: "firmware.bin",
            file_size_bytes: declared_size,
            step_timeout: Duration::from_secs(1),
            cancel: &cancel,
        };

        let client = send_scp_file(&mut local_file, &mut client_stream, &transfer);

        let server = async {
            let mut header = vec![0_u8; expected_header.len()];
            server_stream.read_exact(&mut header).await?;
            server_stream.write_all(&[0]).await?;

            let mut received_payload = vec![0_u8; payload.len()];
            server_stream.read_exact(&mut received_payload).await?;

            Ok::<_, std::io::Error>((header, received_payload))
        };

        let (client_result, server_result) = tokio::join!(client, server);
        let (header, received_payload) = server_result?;

        let error = client_result.unwrap_err();

        assert!(error.to_string().contains("file ended after 5 bytes"));
        assert_eq!(header, expected_header.as_bytes());
        assert_eq!(received_payload, payload);

        Ok(())
    }

    #[tokio::test]
    async fn read_scp_response_returns_remote_error_message() {
        let mut response = b"\x01permission denied\n".as_slice();

        let error = read_scp_response(&mut response, "/remote/firmware.bin", "initial response")
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("recoverable error: permission denied")
        );
    }

    #[tokio::test]
    async fn read_scp_response_rejects_unknown_status_byte() {
        let mut response = b"\x03".as_slice();

        let error = read_scp_response(&mut response, "/remote/firmware.bin", "final response")
            .await
            .unwrap_err();

        assert!(error.to_string().contains("invalid SCP response byte 3"));
    }

    #[test]
    fn record_scp_initial_message_accepts_deferred_ack_data() {
        let mut response = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_status = None;

        let accepted = record_scp_initial_message(
            &mut response,
            &mut stderr,
            &mut exit_status,
            "/remote/firmware.bin",
            ChannelMsg::Data {
                data: vec![0].into(),
            },
        )
        .unwrap();

        assert!(accepted);
        assert_eq!(response, [0]);
        assert!(stderr.is_empty());
        assert_eq!(exit_status, None);
    }

    #[test]
    fn record_scp_initial_message_reports_stderr_and_exit_status() {
        let mut response = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_status = None;

        record_scp_initial_message(
            &mut response,
            &mut stderr,
            &mut exit_status,
            "/remote/firmware.bin",
            ChannelMsg::ExtendedData {
                data: b"scp: command not found".as_slice().into(),
                ext: 1,
            },
        )
        .unwrap();

        record_scp_initial_message(
            &mut response,
            &mut stderr,
            &mut exit_status,
            "/remote/firmware.bin",
            ChannelMsg::ExitStatus { exit_status: 127 },
        )
        .unwrap();

        let error = record_scp_initial_message(
            &mut response,
            &mut stderr,
            &mut exit_status,
            "/remote/firmware.bin",
            ChannelMsg::Close,
        )
        .unwrap_err();

        let message = error.to_string();

        assert!(message.contains("exit status 127"));
        assert!(message.contains("stderr: scp: command not found"));
    }

    #[tokio::test]
    async fn timeout_helper_short_circuits_when_already_cancelled() {
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result: SshResult<()> = with_scp_timeout_and_cancellation(
            "10.0.0.1",
            "/tmp/firmware.bin",
            "SCP local file open",
            Duration::from_secs(1),
            &cancel,
            future::poll_fn(|_| panic!("cancelled operation must not be polled")),
        )
        .await;

        assert!(matches!(result, Err(SshError::Cancelled { .. })));
    }

    #[tokio::test]
    async fn timeout_helper_rejects_stalled_operation() {
        let cancel = CancellationToken::new();

        let result = with_scp_timeout_and_cancellation(
            "10.0.0.1",
            "/tmp/firmware.bin",
            "SCP local file open",
            Duration::from_millis(1),
            &cancel,
            future::pending::<SshResult<()>>(),
        )
        .await;

        assert!(matches!(result, Err(SshError::Timeout { .. })));
    }

    #[tokio::test]
    async fn scp_upload_without_connection_returns_failed_precondition() {
        let client = SshClient::disconnected_for_test("10.0.0.1");

        let error = client
            .scp_upload(
                "/tmp/firmware.bin",
                "/remote/firmware.bin",
                Duration::from_secs(1),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.code,
            crate::utilities::error::ErrorCode::FailedPrecondition
        );
    }
}
