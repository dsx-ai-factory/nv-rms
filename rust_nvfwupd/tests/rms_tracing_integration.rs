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

use std::io;
use std::sync::{Arc, Mutex};

use nvfwupd::cli_schema::{CLISchema, CommandSchema};
use nvfwupd::updcommand::FwUpdCmdBase;
use nvfwupd::util::TraceFlags;
use serde_json::json;
use tracing::Level;
use tracing_subscriber::fmt::writer::MakeWriter;

#[derive(Clone, Default)]
struct CapturedWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl CapturedWriter {
    fn contents(&self) -> String {
        let bytes = self.buffer.lock().expect("capture lock").clone();
        String::from_utf8(bytes).expect("captured tracing output is utf8")
    }
}

struct CapturedWriterHandle {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for CapturedWriterHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer
            .lock()
            .expect("capture lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedWriter {
    type Writer = CapturedWriterHandle;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedWriterHandle {
            buffer: Arc::clone(&self.buffer),
        }
    }
}

fn minimal_command_schema() -> CommandSchema {
    CommandSchema {
        name: "update_fw".to_string(),
        class_name: "FwUpdCmdUpdateFirmware".to_string(),
        require_global_option: false,
        description: String::new(),
        usage: String::new(),
        options: Vec::new(),
    }
}

#[test]
fn rms_style_subscriber_captures_nvfwupd_events_as_structured_fields() {
    let writer = CapturedWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_ansi(false)
        .with_max_level(Level::INFO)
        .with_writer(writer.clone())
        .finish();
    let schema = CLISchema::new();
    let base = FwUpdCmdBase {
        schema: &schema,
        exec_name: "nvfwupd".to_string(),
        cmd_name: "update_fw".to_string(),
        cmd_schema: minimal_command_schema(),
        global_options: Vec::new(),
        args: Vec::new(),
        trace: TraceFlags::new(false, false),
        config_parser: None,
        g_verbose: false,
    };
    let json_output = json!({});

    tracing::subscriber::with_default(subscriber, || {
        base.log_parallel_thread_count(Some(&json_output));
    });

    let output = writer.contents();
    let event: serde_json::Value =
        serde_json::from_str(output.lines().next().expect("one tracing event"))
            .expect("structured tracing event");

    assert_eq!(event["level"], "INFO");
    assert_eq!(
        event["fields"]["message"],
        "Using default ParallelThreadCount=10"
    );
    assert_eq!(event["fields"]["indent"], 0);
    assert_eq!(event["fields"]["log_only"], true);
    assert_eq!(event["target"], "nvfwupd::updcommand");
}
