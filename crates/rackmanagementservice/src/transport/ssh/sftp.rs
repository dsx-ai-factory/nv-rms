/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

//! SFTP transfer and metadata helpers for an authenticated SSH session.

use std::future::Future;
use std::time::{Duration, Instant};

use russh::{ChannelMsg, client};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::OpenFlags;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use super::client::{SFTP_UPLOAD_BUFFER_SIZE_BYTES, SftpUploadOptions, SshClient, SshHandler};
use super::error::{SshError, SshResult};
use super::request::{ChannelRequestFailureCode, wait_for_channel_request_confirmation};
use crate::utilities::error::Result;

/// Emit upload progress at least this often while bytes continue flowing.
const SFTP_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Emit upload progress after this many newly written bytes.
const SFTP_PROGRESS_LOG_BYTES: u64 = 64 * 1024 * 1024;

/// Throughput logs use MiB/s because NVOS image sizes are binary-sized files.
const MIB_BYTES: f64 = 1024.0 * 1024.0;

impl SshClient {
    /// Upload a local file to the remote host via SFTP.
    ///
    /// Uses a fixed 512 KiB buffer and the default step timeout, capped by
    /// `timeout`, with `timeout` as the overall upload timeout.
    ///
    /// `cancel` provides cooperative cancellation: the remote-facing steps
    /// (session setup, remote open, each chunk write, and the final flush) race
    /// against the token so a graceful shutdown interrupts an in-flight upload
    /// promptly instead of letting it run against a live target until the
    /// timeout elapses. On cancellation the transfer unwinds, dropping the SFTP
    /// session and channel. Pass [`CancellationToken::new`] (never cancelled)
    /// when the caller has no cancellation source.
    ///
    /// # Errors
    ///
    /// Returns `FailedPrecondition` if the client is not connected, `NotFound`
    /// if `local_path` cannot be opened, `Timeout` if the upload exceeds
    /// `timeout` or one upload step exceeds the effective step timeout, or
    /// `Cancelled` if `cancel` is triggered mid-transfer,
    /// `Internal`/`Unavailable` for SSH and SFTP protocol failures.
    pub async fn sftp_upload(
        &self,
        local_path: &str,
        remote_path: &str,
        timeout: Duration,
        cancel: CancellationToken,
    ) -> Result<()> {
        let default_options = SftpUploadOptions::default();
        self.sftp_upload_with_options(
            local_path,
            remote_path,
            SftpUploadOptions {
                overall_timeout: timeout,
                step_timeout: default_options.step_timeout.min(timeout),
            },
            cancel,
        )
        .await
    }

    /// Upload a local file to the remote host via SFTP using explicit tunables.
    ///
    /// Streams from disk with a reusable buffer, never holding the entire file
    /// in memory.
    /// Emits structured start, progress, and completion logs with byte counts
    /// and throughput rates.
    ///
    /// # Post-upload size check
    ///
    /// After the final flush, the remote file is `stat`-ed and its reported size
    /// is compared to the number of bytes accepted by the server's writes. This
    /// is a **best-effort, warn-only** truncation guard: SSH already protects the
    /// wire with a per-packet MAC/AEAD and every SFTP write is acknowledged, so
    /// this check only exists to surface a short/altered remote file from
    /// client-side early-exit, cancellation, or a server that quietly accepted
    /// fewer bytes. A mismatch, a size the server does not report, or a failed
    /// `stat` all emit a `warn!` but never fail an otherwise-successful upload.
    /// It is a size-only check and does not detect same-length corruption.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `options` are out of range,
    /// `FailedPrecondition` if the client is not connected, `NotFound` if
    /// `local_path` cannot be opened, `Timeout` if the upload exceeds
    /// `options.overall_timeout` or one upload step exceeds
    /// `options.step_timeout`, or `Internal`/`Unavailable` for SSH and SFTP
    /// protocol failures.
    pub async fn sftp_upload_with_options(
        &self,
        local_path: &str,
        remote_path: &str,
        options: SftpUploadOptions,
        cancel: CancellationToken,
    ) -> Result<()> {
        options.validate()?;
        let handle = self.handle.as_ref().ok_or(SshError::NotConnected)?;

        let host = self.host.as_str();
        let upload_timeout = options.overall_timeout;
        let timeout_secs = upload_timeout.as_secs();
        let step_timeout_secs = options.step_timeout.as_secs();
        let buffer_size_bytes = SFTP_UPLOAD_BUFFER_SIZE_BYTES;

        let local_file = tokio::fs::File::open(local_path).await.map_err(|e| {
            let error = e.to_string();
            tracing::warn!(
                host,
                local_path,
                error = &error,
                "SFTP local file open failed"
            );

            SshError::LocalFileOpenFailed {
                path: local_path.to_owned(),
                details: error,
            }
        })?;
        // File size is observability-only; do not fail an upload if metadata
        // cannot be read after the file has already opened.
        let local_file_size_bytes = local_file
            .metadata()
            .await
            .ok()
            .map(|metadata| metadata.len());

        tracing::info!(
            host,
            local_path,
            remote_path,
            timeout_secs,
            step_timeout_secs,
            buffer_size_bytes,
            file_size_bytes = local_file_size_bytes.unwrap_or(0),
            file_size_known = local_file_size_bytes.is_some(),
            "SFTP upload starting"
        );

        with_sftp_timeout_and_cancellation(
            host,
            remote_path,
            "SFTP upload",
            upload_timeout,
            cancel.clone(),
            async {
                let step_timeout = options.step_timeout;

                let sftp = with_sftp_timeout_and_cancellation(
                    host,
                    remote_path,
                    "SFTP session setup",
                    step_timeout,
                    cancel.clone(),
                    open_sftp_session(handle, host, remote_path),
                )
                .await?;

                let mut remote_file = with_sftp_timeout_and_cancellation(
                    host,
                    remote_path,
                    "SFTP remote file open",
                    step_timeout,
                    cancel.clone(),
                    async {
                        sftp.open_with_flags(
                            remote_path,
                            OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
                        )
                        .await
                        .map_err(|e| {
                            let error = e.to_string();
                            tracing::warn!(
                                host,
                                remote_path,
                                error = &error,
                                "SFTP remote file open failed"
                            );

                            SshError::SftpOperationFailed {
                                operation: "remote file open",
                                path: remote_path.to_owned(),
                                details: error,
                            }
                        })
                    },
                )
                .await?;

                let mut reader = tokio::io::BufReader::new(local_file);
                let mut buf = vec![0_u8; buffer_size_bytes];
                let mut progress = SftpUploadProgress::new(local_file_size_bytes, Instant::now());

                loop {
                    let n = with_sftp_timeout_and_cancellation(
                        host,
                        local_path,
                        "SFTP local file read",
                        step_timeout,
                        cancel.clone(),
                        async {
                            reader.read(&mut buf).await.map_err(|e| {
                                let error = e.to_string();
                                tracing::warn!(
                                    host,
                                    local_path,
                                    error = &error,
                                    "SFTP local file read failed"
                                );

                                SshError::SftpOperationFailed {
                                    operation: "local file read",
                                    path: local_path.to_owned(),
                                    details: error,
                                }
                            })
                        },
                    )
                    .await?;

                    if n == 0 {
                        break;
                    }

                    let chunk = buf.get(..n).ok_or_else(|| SshError::SftpOperationFailed {
                        operation: "local file read",
                        path: local_path.to_owned(),
                        details: format!("read {n} bytes into {} byte buffer", buf.len()),
                    })?;

                    with_sftp_timeout_and_cancellation(
                        host,
                        remote_path,
                        "SFTP remote file write",
                        step_timeout,
                        cancel.clone(),
                        async {
                            remote_file.write_all(chunk).await.map_err(|e| {
                                let error = e.to_string();
                                tracing::warn!(
                                    host,
                                    remote_path,
                                    bytes = n,
                                    error = &error,
                                    "SFTP remote file write failed"
                                );

                                SshError::SftpOperationFailed {
                                    operation: "remote file write",
                                    path: remote_path.to_owned(),
                                    details: error,
                                }
                            })
                        },
                    )
                    .await?;

                    // Count bytes only after the remote write succeeds so progress
                    // logs reflect data accepted by the SFTP server.
                    progress.record(n);
                    let now = Instant::now();

                    if progress.should_log(now) {
                        let snapshot = progress.snapshot(now);

                        log_sftp_upload_progress(host, local_path, remote_path, &snapshot);
                        progress.mark_logged(now);
                    }
                }

                with_sftp_timeout_and_cancellation(
                    host,
                    remote_path,
                    "SFTP remote file flush",
                    step_timeout,
                    cancel,
                    async {
                        remote_file.flush().await.map_err(|e| {
                            let error = e.to_string();
                            tracing::warn!(
                                host,
                                remote_path,
                                error = &error,
                                "SFTP remote file flush failed"
                            );

                            SshError::SftpOperationFailed {
                                operation: "remote file flush",
                                path: remote_path.to_owned(),
                                details: error,
                            }
                        })
                    },
                )
                .await?;

                // Best-effort truncation guard. SSH already protects wire
                // integrity (per-packet MAC/AEAD) and every write is
                // acknowledged, so this only catches a short/altered remote
                // file from client-side early-exit, cancellation, or a server
                // that quietly accepted fewer bytes. It is observability-only:
                // never fail an otherwise-successful upload on this check.
                let bytes_sent = progress.bytes_sent;
                match tokio::time::timeout(step_timeout, sftp.metadata(remote_path)).await {
                    Ok(Ok(metadata)) => match metadata.size {
                        Some(remote_size) if remote_size == bytes_sent => {
                            tracing::debug!(
                                host,
                                remote_path,
                                bytes_sent,
                                remote_size,
                                "SFTP upload remote size matches bytes sent"
                            );
                        }
                        Some(remote_size) => {
                            tracing::warn!(
                                host,
                                remote_path,
                                bytes_sent,
                                remote_size,
                                "SFTP upload remote size does not match bytes sent; \
                                 remote file may be truncated or altered"
                            );
                        }
                        None => {
                            tracing::warn!(
                                host,
                                remote_path,
                                bytes_sent,
                                "SFTP server did not report a size after upload; \
                                 could not verify transferred size"
                            );
                        }
                    },
                    Ok(Err(e)) => {
                        tracing::warn!(
                            host,
                            remote_path,
                            bytes_sent,
                            error = %e.to_string(),
                            "failed to stat remote file after SFTP upload; \
                             could not verify transferred size"
                        );
                    }
                    Err(_) => {
                        tracing::warn!(
                            host,
                            remote_path,
                            bytes_sent,
                            step_timeout_secs,
                            "remote size verification timed out after SFTP upload; \
                             could not verify transferred size"
                        );
                    }
                }

                let snapshot = progress.snapshot(Instant::now());

                log_sftp_upload_completed(
                    host,
                    local_path,
                    remote_path,
                    &snapshot,
                    buffer_size_bytes,
                );

                Ok(())
            },
        )
        .await?;

        Ok(())
    }

    /// Query the size of a remote file via SFTP.
    ///
    /// If the server returns metadata without a size, this method returns `0`
    /// (see the WARNING in the body: a `0` result cannot be distinguished from
    /// a genuine zero-byte file).
    ///
    /// # Errors
    ///
    /// Returns `FailedPrecondition` if the client is not connected, `NotFound`
    /// if `remote_path` cannot be read, `Timeout` if the metadata query exceeds
    /// [`SshClient::DEFAULT_TIMEOUT`], or `Internal`/`Unavailable` for SSH and
    /// SFTP protocol failures.
    pub async fn sftp_file_size(&self, remote_path: &str) -> Result<u64> {
        let handle = self.handle.as_ref().ok_or(SshError::NotConnected)?;

        let host = self.host.as_str();

        let timeout = SshClient::DEFAULT_TIMEOUT;
        let timeout_secs = timeout.as_secs();

        let size = tokio::time::timeout(timeout, async {
            let sftp = open_sftp_session(handle, host, remote_path).await?;

            let metadata =
                sftp.metadata(remote_path)
                    .await
                    .map_err(|e| SshError::RemoteFileNotFound {
                        path: remote_path.to_owned(),
                        details: e.to_string(),
                    })?;

            // When the server omits the size in its metadata we return `0`
            // rather than erroring, preserving historical behavior where
            // callers treat a size mismatch as "not staged" and re-upload.
            //
            // WARNING: `0` here is indistinguishable from a genuine zero-byte
            // file whose size *was* reported as 0. Callers must therefore treat
            // a `0` result only as "size unknown / re-upload", never as proof
            // that the remote file is empty.
            Ok::<u64, SshError>(metadata.size.unwrap_or(0))
        })
        .await
        .map_err(|_| {
            tracing::warn!(
                host,
                remote_path,
                timeout_secs,
                "SFTP metadata query timed out"
            );
            SshError::timeout("SFTP metadata query", host, timeout)
        })??;

        tracing::debug!(
            host,
            remote_path,
            size,
            timeout_secs,
            "SFTP metadata query completed"
        );

        Ok(size)
    }
}

fn log_sftp_upload_progress(
    host: &str,
    local_path: &str,
    remote_path: &str,
    snapshot: &SftpUploadProgressSnapshot,
) {
    let percent_complete = snapshot.percent_complete.unwrap_or(0.0);

    tracing::info!(
        host,
        local_path,
        remote_path,
        bytes_sent = snapshot.bytes_sent,
        file_size_bytes = snapshot.total_bytes.unwrap_or(0),
        file_size_known = snapshot.total_bytes.is_some(),
        elapsed_secs = %format_args!("{:.2}", snapshot.elapsed.as_secs_f64()),
        average_mib_per_sec = %format_args!("{:.2}", snapshot.average_mib_per_second),
        recent_mib_per_sec = %format_args!("{:.2}", snapshot.recent_mib_per_second),
        percent_complete = %format_args!("{percent_complete:.2}"),
        "SFTP upload progress"
    );
}

fn log_sftp_upload_completed(
    host: &str,
    local_path: &str,
    remote_path: &str,
    snapshot: &SftpUploadProgressSnapshot,
    buffer_size_bytes: usize,
) {
    let percent_complete = snapshot.percent_complete.unwrap_or(0.0);

    tracing::info!(
        host,
        local_path,
        remote_path,
        bytes_sent = snapshot.bytes_sent,
        file_size_bytes = snapshot.total_bytes.unwrap_or(0),
        file_size_known = snapshot.total_bytes.is_some(),
        elapsed_secs = %format_args!("{:.2}", snapshot.elapsed.as_secs_f64()),
        average_mib_per_sec = %format_args!("{:.2}", snapshot.average_mib_per_second),
        recent_mib_per_sec = %format_args!("{:.2}", snapshot.recent_mib_per_second),
        percent_complete = %format_args!("{percent_complete:.2}"),
        buffer_size_bytes,
        "SFTP upload completed"
    );
}

/// Upload progress counters used to throttle SFTP observability logs.
///
/// Bytes are recorded only after a remote write succeeds, so reported progress
/// reflects data accepted by the SFTP server. `last_log_at` and
/// `last_log_bytes` define the recent-throughput window for the next progress
/// log. `last_recent_mib_per_second` preserves the latest non-empty window for
/// the completion log when the final chunk also emitted a progress log.
struct SftpUploadProgress {
    total_bytes: Option<u64>,
    bytes_sent: u64,
    started_at: Instant,
    last_log_at: Instant,
    last_log_bytes: u64,
    last_recent_mib_per_second: f64,
}

/// Immutable view of upload progress at one instant.
///
/// Average throughput covers the whole upload so far. Recent throughput covers
/// bytes written since the last emitted progress log.
struct SftpUploadProgressSnapshot {
    total_bytes: Option<u64>,
    bytes_sent: u64,
    elapsed: Duration,
    average_mib_per_second: f64,
    recent_mib_per_second: f64,
    percent_complete: Option<f64>,
}

impl SftpUploadProgress {
    /// Start measuring a single upload at `now`.
    fn new(total_bytes: Option<u64>, now: Instant) -> Self {
        Self {
            total_bytes,
            bytes_sent: 0,
            started_at: now,
            last_log_at: now,
            last_log_bytes: 0,
            last_recent_mib_per_second: 0.0,
        }
    }

    /// Add bytes after they were accepted by the remote SFTP write.
    fn record(&mut self, bytes_sent: usize) {
        self.bytes_sent += bytes_sent as u64;
    }

    /// Return `true` when enough new time or bytes have accrued to log again.
    fn should_log(&self, now: Instant) -> bool {
        self.bytes_sent > self.last_log_bytes
            && (self.bytes_sent - self.last_log_bytes >= SFTP_PROGRESS_LOG_BYTES
                || now.duration_since(self.last_log_at) >= SFTP_PROGRESS_LOG_INTERVAL)
    }

    /// Capture total and recent throughput without mutating log state.
    fn snapshot(&self, now: Instant) -> SftpUploadProgressSnapshot {
        let elapsed = now.duration_since(self.started_at);
        let recent_elapsed = now.duration_since(self.last_log_at);
        let recent_bytes = self.bytes_sent - self.last_log_bytes;

        let recent_mib_per_second = if recent_bytes == 0 {
            self.last_recent_mib_per_second
        } else {
            mib_per_second(recent_bytes, recent_elapsed)
        };

        let percent_complete = self.total_bytes.and_then(|total| {
            if total == 0 {
                None
            } else {
                Some((self.bytes_sent as f64 / total as f64) * 100.0)
            }
        });

        SftpUploadProgressSnapshot {
            total_bytes: self.total_bytes,
            bytes_sent: self.bytes_sent,
            elapsed,
            average_mib_per_second: mib_per_second(self.bytes_sent, elapsed),
            recent_mib_per_second,
            percent_complete,
        }
    }

    /// Reset the recent-throughput and log-throttling window after a progress log.
    fn mark_logged(&mut self, now: Instant) {
        let recent_elapsed = now.duration_since(self.last_log_at);
        let recent_bytes = self.bytes_sent - self.last_log_bytes;

        if recent_bytes != 0 {
            self.last_recent_mib_per_second = mib_per_second(recent_bytes, recent_elapsed);
        }

        self.last_log_at = now;
        self.last_log_bytes = self.bytes_sent;
    }
}

/// Convert bytes over elapsed wall-clock time to MiB/s for log fields.
fn mib_per_second(bytes: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }

    (bytes as f64 / MIB_BYTES) / elapsed.as_secs_f64()
}

/// Apply a timeout to an SFTP operation and race it against cancellation.
///
/// The upload path caps each operation two ways at once. The caller-supplied
/// upload timeout caps the whole transfer, while
/// [`SftpUploadOptions::step_timeout`] caps each setup, open, read, write, and
/// flush step so stalls are detected promptly.
///
/// In parallel, the operation races `cancel` with `biased` priority so an
/// already-cancelled token short-circuits before the future is polled. On
/// cancellation the future is dropped (unwinding the in-flight transfer) and a
/// [`SshError::Cancelled`] is returned so the caller can distinguish shutdown
/// from timeouts and other failures.
async fn with_sftp_timeout_and_cancellation<T>(
    host: &str,
    path: &str,
    operation: &'static str,
    timeout: Duration,
    cancel: CancellationToken,
    future: impl Future<Output = SshResult<T>>,
) -> SshResult<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            tracing::info!(host, path, operation, "SFTP operation cancelled");
            Err(SshError::Cancelled {
                operation,
                host: host.to_owned(),
            })
        }
        result = tokio::time::timeout(timeout, future) => {
            result.map_err(|_| {
                tracing::warn!(
                    host,
                    path,
                    operation,
                    timeout_secs = timeout.as_secs(),
                    "SFTP operation timed out"
                );
                SshError::timeout(operation, host, timeout)
            })?
        }
    }
}

/// Open an SFTP subsystem over an existing SSH session.
///
/// Channel-open and subsystem-request errors are treated as service
/// unavailability. Session initialization errors are treated as SFTP operation
/// failures because the subsystem accepted the request but could not be used.
async fn open_sftp_session(
    handle: &client::Handle<SshHandler>,
    host: &str,
    remote_path: &str,
) -> SshResult<SftpSession> {
    let mut channel = handle.channel_open_session().await.map_err(|e| {
        let error = e.to_string();

        tracing::warn!(
            host,
            remote_path,
            error = &error,
            "SSH channel open for SFTP failed"
        );

        SshError::Unavailable {
            operation: "SFTP setup",
            details: error,
        }
    })?;

    channel.request_subsystem(true, "sftp").await.map_err(|e| {
        let error = e.to_string();

        tracing::warn!(
            host,
            remote_path,
            error = &error,
            "SFTP subsystem request failed"
        );

        SshError::Unavailable {
            operation: "SFTP setup",
            details: error,
        }
    })?;

    let deferred_messages = wait_for_channel_request_confirmation(
        &mut channel,
        "SFTP subsystem request",
        ChannelRequestFailureCode::Unavailable,
    )
    .await?;

    let deferred_message_count = deferred_messages.len();

    // Why this guard exists:
    //
    // 1. `request_subsystem(true, "sftp")` only enqueues the SSH channel
    //    request. The server's Success or Failure reply arrives later on the
    //    same channel queue that will also carry SFTP protocol data.
    // 2. `wait_for_channel_request_confirmation` must read from that queue
    //    before `russh-sftp` owns the channel stream.
    // 3. A normal SFTP server should not send payload before the client sends
    //    the SFTP init packet, so early Data is anomalous rather than expected.
    // 4. Silently dropping early payload would corrupt the SFTP protocol
    //    stream, while buffering it adds complexity for a state that should not
    //    occur.
    //
    // Therefore, allow harmless channel metadata and fail explicitly on payload
    // or control messages before handing the stream to `russh-sftp`.
    validate_no_sftp_deferred_payload(remote_path, deferred_messages)?;

    if deferred_message_count != 0 {
        tracing::debug!(
            host,
            remote_path,
            deferred_messages = deferred_message_count,
            "SFTP subsystem request received harmless early channel messages"
        );
    }

    SftpSession::new(channel.into_stream()).await.map_err(|e| {
        let error = e.to_string();

        tracing::warn!(
            host,
            remote_path,
            error = &error,
            "SFTP session init failed"
        );

        SshError::SftpOperationFailed {
            operation: "session init",
            path: remote_path.to_owned(),
            details: error,
        }
    })
}

/// Reject early SFTP payload before `russh-sftp` owns the channel stream.
fn validate_no_sftp_deferred_payload(
    remote_path: &str,
    deferred_messages: Vec<ChannelMsg>,
) -> SshResult<()> {
    for message in deferred_messages {
        match message {
            ChannelMsg::WindowAdjusted { .. } => {}
            unexpected => {
                return Err(SshError::SftpOperationFailed {
                    operation: "session init",
                    path: remote_path.to_owned(),
                    details: format!(
                        "unexpected early channel message before SFTP stream handoff: {unexpected:?}"
                    ),
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use russh::ChannelMsg;
    use tracing_subscriber::prelude::*;

    use super::*;

    #[derive(Clone, Default)]
    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl TestWriter {
        fn text(&self) -> String {
            let guard = self.0.lock().unwrap();
            String::from_utf8_lossy(&guard).into_owned()
        }
    }

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = self.0.lock().unwrap();
            std::io::Write::write(&mut *guard, buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn sftp_upload_progress_logs_after_byte_threshold() {
        let start = Instant::now();
        let mut progress = SftpUploadProgress::new(Some(SFTP_PROGRESS_LOG_BYTES * 2), start);

        progress.record((SFTP_PROGRESS_LOG_BYTES - 1) as usize);

        assert!(!progress.should_log(start + Duration::from_secs(1)));

        progress.record(1);

        assert!(progress.should_log(start + Duration::from_secs(1)));

        let snapshot = progress.snapshot(start + Duration::from_secs(1));

        assert_eq!(snapshot.bytes_sent, SFTP_PROGRESS_LOG_BYTES);
        assert_eq!(snapshot.recent_mib_per_second, 64.0);
        assert_eq!(snapshot.percent_complete, Some(50.0));

        progress.mark_logged(start + Duration::from_secs(1));

        assert!(!progress.should_log(start + Duration::from_secs(2)));
    }

    #[test]
    fn sftp_upload_progress_logs_after_time_threshold() {
        let start = Instant::now();
        let mut progress = SftpUploadProgress::new(Some(1024), start);

        progress.record(1024);

        assert!(!progress.should_log(start + SFTP_PROGRESS_LOG_INTERVAL / 2));
        assert!(progress.should_log(start + SFTP_PROGRESS_LOG_INTERVAL));
    }

    #[test]
    fn sftp_upload_completion_preserves_recent_rate_after_terminal_progress_log() {
        let start = Instant::now();
        let mut progress = SftpUploadProgress::new(Some(SFTP_PROGRESS_LOG_BYTES), start);

        progress.record(SFTP_PROGRESS_LOG_BYTES as usize);

        let progress_snapshot = progress.snapshot(start + Duration::from_secs(1));

        progress.mark_logged(start + Duration::from_secs(1));

        let completion_snapshot = progress.snapshot(start + Duration::from_secs(2));

        assert_eq!(
            completion_snapshot.recent_mib_per_second,
            progress_snapshot.recent_mib_per_second
        );
    }

    #[test]
    fn sftp_upload_logs_format_progress_fields() {
        let writer = TestWriter::default();
        let cloned_writer = writer.clone();
        let layer = crate::logging::logfmt::layer()
            .with_writer(Arc::new(move || Box::new(cloned_writer.clone())));

        let subscriber = tracing_subscriber::registry().with(layer);
        let start = Instant::now();
        let mut unknown_progress = SftpUploadProgress::new(None, start);
        let mut known_progress = SftpUploadProgress::new(Some(20 * 1024), start);

        unknown_progress.record(10 * 1024);
        known_progress.record(10 * 1024);

        let unknown_snapshot = unknown_progress.snapshot(start + Duration::from_secs(1));
        let known_snapshot = known_progress.snapshot(start + Duration::from_secs(1));

        tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();

            log_sftp_upload_completed(
                "10.0.0.1",
                "/tmp/fw.bin",
                "/remote/fw.bin",
                &unknown_snapshot,
                SFTP_UPLOAD_BUFFER_SIZE_BYTES,
            );

            log_sftp_upload_progress("10.0.0.1", "/tmp/fw.bin", "/remote/fw.bin", &known_snapshot);
        });

        let output = writer.text();
        let lines: Vec<_> = output.lines().collect();

        let [completion_zero_percent_line, progress_with_percent_line] = lines.as_slice() else {
            panic!("expected completion and progress log lines: {output}");
        };

        for line in [completion_zero_percent_line, progress_with_percent_line] {
            assert!(
                line.contains("elapsed_secs=1.00")
                    && line.contains("average_mib_per_sec=0.01")
                    && line.contains("recent_mib_per_sec=0.01"),
                "line should format progress fields: {line}"
            );
        }

        assert!(
            completion_zero_percent_line.contains(r#"msg="SFTP upload completed""#)
                && completion_zero_percent_line.contains("percent_complete=0.00"),
            "unexpected completion log fields: {completion_zero_percent_line}"
        );

        assert!(
            progress_with_percent_line.contains(r#"msg="SFTP upload progress""#)
                && progress_with_percent_line.contains("percent_complete=50.00"),
            "unexpected progress log fields: {progress_with_percent_line}"
        );
    }

    #[test]
    fn mib_per_second_returns_zero_for_zero_elapsed() {
        assert_eq!(mib_per_second(1024 * 1024, Duration::ZERO), 0.0);
    }

    #[test]
    fn validate_no_sftp_deferred_payload_accepts_window_adjustments() {
        validate_no_sftp_deferred_payload(
            "/remote/fw.bin",
            vec![ChannelMsg::WindowAdjusted { new_size: 1024 }],
        )
        .unwrap();
    }

    #[test]
    fn validate_no_sftp_deferred_payload_rejects_data_messages() {
        let err = validate_no_sftp_deferred_payload(
            "/remote/fw.bin",
            vec![ChannelMsg::Data {
                data: b"payload".as_slice().into(),
            }],
        )
        .unwrap_err();

        match err {
            SshError::SftpOperationFailed {
                operation,
                path,
                details,
            } => {
                assert_eq!(operation, "session init");
                assert_eq!(path, "/remote/fw.bin");
                assert!(details.contains("Data"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_sftp_timeout_and_cancellation_returns_completed_value() {
        let cancel = CancellationToken::new();
        let value = with_sftp_timeout_and_cancellation(
            "10.0.0.1",
            "/remote/fw.bin",
            "SFTP remote file write",
            Duration::from_secs(1),
            cancel,
            async { Ok::<u32, SshError>(7) },
        )
        .await
        .unwrap();

        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn with_sftp_timeout_and_cancellation_rejects_stalled_future() {
        let cancel = CancellationToken::new();
        let err = with_sftp_timeout_and_cancellation(
            "10.0.0.1",
            "/remote/fw.bin",
            "SFTP remote file write",
            Duration::from_millis(1),
            cancel,
            future::pending::<SshResult<()>>(),
        )
        .await
        .unwrap_err();

        match err {
            SshError::Timeout {
                operation, host, ..
            } => {
                assert_eq!(operation, "SFTP remote file write");
                assert_eq!(host, "10.0.0.1");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_sftp_timeout_and_cancellation_short_circuits_when_already_cancelled() {
        let cancel = CancellationToken::new();
        cancel.cancel();

        // The inner future never resolves and the timeout is effectively
        // infinite, so returning at all proves the token was observed with
        // `biased` priority before the operation was polled.
        let err = with_sftp_timeout_and_cancellation(
            "10.0.0.1",
            "/remote/fw.bin",
            "SFTP remote file write",
            Duration::from_secs(3600),
            cancel,
            future::pending::<SshResult<()>>(),
        )
        .await
        .unwrap_err();

        match err {
            SshError::Cancelled { operation, host } => {
                assert_eq!(operation, "SFTP remote file write");
                assert_eq!(host, "10.0.0.1");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_sftp_timeout_and_cancellation_wakes_on_signal_mid_operation() {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            cancel_clone.cancel();
        });

        let start = std::time::Instant::now();
        let err = with_sftp_timeout_and_cancellation(
            "10.0.0.1",
            "/remote/fw.bin",
            "SFTP remote file write",
            Duration::from_secs(3600),
            cancel,
            future::pending::<SshResult<()>>(),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, SshError::Cancelled { .. }),
            "expected cancellation, got: {err:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "cancellation should interrupt promptly"
        );
    }
}
