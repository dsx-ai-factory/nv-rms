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
//! Tests of the crate as a caller sees it.
//!
//! These tests drive the crate through its public API only: `Executable`,
//! `Target`, `CollectRequest`, `Collection`, `Error`, and the rest of what
//! `lib.rs` exports. nvdebug is played by `fake-nvdebug.sh`, a shell script
//! next to this file. It records what the crate passed it into the output
//! directory, and the first collector id tells it what to do. The processes
//! it runs and the files it writes are real.
//!
//! They are not integration tests, so they do not live in `tests/`. An
//! integration test runs against the real nvdebug, with a real BMC or the
//! sim in the loop, and `tests/` is kept for those. A stub there would read
//! as if it were the real thing.
//!
//! Inside the crate, the compiler does not stop a test from reaching a
//! private item. The rule holds by review instead: use only what a caller
//! can use. A test of a private detail belongs in the module it tests, as
//! the ones in `dut_dir.rs` do, and says why a caller cannot reach it.
//!
//! The tests run in parallel. Each has a temp dir and an output directory of
//! its own. The script is checked in and never written by a test, so no
//! exec of it can fail with "Text file busy". No test may call
//! `sweep_stale_dut_dirs`: it removes the DUT directories with this
//! process's id, which would be the live ones of the tests next to it.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use secrecy::SecretString;

use crate::{
    Bmc, BmcHost, CancellationToken, CollectRequest, CollectorId, Error, Executable, Target,
};

/// The checked-in stand-in for nvdebug (`fake-nvdebug.sh`). It records into
/// the output directory, then exits with the first collector id as its code,
/// or holds when that id is HOLD.
fn fake_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src/fake-nvdebug.sh")
}

fn fake() -> Executable {
    Executable::new(fake_path()).expect("absolute")
}

fn bmc(host: &str, password: &str) -> Bmc {
    Bmc::new(
        BmcHost::parse(host).expect("a host"),
        "admin".to_owned(),
        SecretString::from(password),
    )
    .expect("valid bmc")
}

fn target(name: &str) -> Target {
    Target::new(name, bmc("10.1.2.30", "p"), "GB200 NVL").expect("valid target")
}

/// A one-target request whose collector ids tell the fake what to do.
fn request(out: &Path, ids: &[&str], timeout: Duration) -> CollectRequest {
    let ids = ids
        .iter()
        .map(|id| CollectorId::new(*id).expect("valid"))
        .collect();
    CollectRequest::new(vec![target("tray07")], ids, out, timeout).expect("valid request")
}

/// A temp dir with every link in its path resolved. nvdebug refuses an
/// output path through a link, and macOS's temp root is behind one.
fn real_tempdir() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().canonicalize().expect("resolved");
    (dir, path)
}

/// Waits until the fake has written `file`.
async fn written(file: &Path) {
    while std::fs::metadata(file).map_or(true, |m| m.len() == 0) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn line_in(file: &Path) -> String {
    std::fs::read_to_string(file)
        .expect("written by the fake")
        .trim()
        .to_owned()
}

// What each constructor accepts and what it refuses, before any run.

#[test]
fn bmc_host_takes_an_ip_or_a_dns_name() {
    for text in ["10.0.0.1", "fd00::1"] {
        let host = BmcHost::parse(text).expect(text);
        assert!(matches!(host, BmcHost::Ip(_)), "{text}");
        assert_eq!(host.to_string(), text);
    }
    for text in ["bmc-07", "bmc-07.rack1.example.com", "bmc_07.example"] {
        let host = BmcHost::parse(text).expect(text);
        assert!(matches!(host, BmcHost::Name(_)), "{text}");
        assert_eq!(host.to_string(), text);
    }
}

#[test]
fn bmc_host_refuses_what_is_not_a_host() {
    for text in [
        "",
        " 10.0.0.1",
        "bmc 07",
        "[fd00::1]",
        "fe80::1%eth0",
        "10.0.0.1:443",
        "bmc-07:443",
        "admin@bmc-07",
        "bmc-07/redfish",
        "bmc-07%2e",
        "bmc-07..example",
        "-bmc-07",
        "bmc-07-",
        "10.0.0.256",
    ] {
        assert!(
            matches!(BmcHost::parse(text), Err(Error::Invalid { .. })),
            "{text:?}"
        );
    }
}

#[test]
fn bmc_refuses_an_empty_user_or_password() {
    for (user, password) in [("", "p"), ("admin", "")] {
        let host = BmcHost::parse("10.1.2.30").expect("an IP address");
        let result = Bmc::new(host, user.to_owned(), SecretString::from(password));
        assert!(matches!(result, Err(Error::Invalid { .. })), "{user:?}");
    }
}

#[test]
fn target_refuses_names_nvdebug_would_escape_merge_or_misread() {
    for name in [
        "",
        "a/b",
        "a b",
        "a\\b",
        "tray'07",
        "a!",
        ".",
        "..",
        "DUT_Defaults",
    ] {
        let result = Target::new(name, bmc("10.1.2.30", "p"), "GB200 NVL");
        assert!(matches!(result, Err(Error::Invalid { .. })), "{name:?}");
    }
    for name in ["tray07", "tray-07", "tray_07", "tray.07", "..."] {
        let result = Target::new(name, bmc("10.1.2.30", "p"), "GB200 NVL");
        assert!(result.is_ok(), "{name:?}");
    }
}

#[test]
fn executable_path_is_absolute() {
    assert!(Executable::new("/opt/nvdebug").is_ok());
    assert!(matches!(
        Executable::new("nvdebug"),
        Err(Error::Invalid { .. })
    ));
}

#[test]
fn collector_id_refuses_separators_that_would_split_the_argument() {
    for id in ["", "R1 R2", "R1,R2", "R\t1"] {
        assert!(
            matches!(CollectorId::new(id), Err(Error::Invalid { .. })),
            "{id:?}"
        );
    }
    assert_eq!(CollectorId::new("R1").expect("valid").to_string(), "R1");
}

#[test]
fn request_refuses_what_nvdebug_or_the_spawn_would_misread() {
    let id = || vec![CollectorId::new("R1").expect("valid")];
    let second = Duration::from_secs(1);

    let empty_targets = CollectRequest::new(vec![], id(), "/out", second);
    let empty_ids = CollectRequest::new(vec![target("t")], vec![], "/out", second);
    let same_name = CollectRequest::new(vec![target("a"), target("a")], id(), "/out", second);
    let relative = CollectRequest::new(vec![target("t")], id(), "out", second);
    let no_time = CollectRequest::new(vec![target("t")], id(), "/out", Duration::ZERO);

    for result in [empty_targets, empty_ids, same_name, relative, no_time] {
        assert!(matches!(result, Err(Error::Invalid { .. })));
    }
    let two = CollectRequest::new(vec![target("a"), target("b")], id(), "/out", second);
    assert!(two.is_ok());
}

// Runs against the fake: what a caller gets back, what nvdebug is given,
// and what is left after a run. How the OS treats nvdebug's processes after
// a stop is for the integration tests.

const SECOND: Duration = Duration::from_secs(1);

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path)
        .expect("exists")
        .permissions()
        .mode()
        & 0o777
}

/// The DUT file's directory, as the fake recorded it.
fn dut_dir(out: &Path) -> PathBuf {
    PathBuf::from(line_in(&out.join("dut_path")))
        .parent()
        .expect("a directory")
        .to_owned()
}

#[tokio::test]
async fn version_is_unavailable_when_nvdebug_cannot_be_used() {
    let cases = [
        ("exits non-zero", "/usr/bin/false"),
        ("not a regular file", "/tmp"),
        ("missing", "/nonexistent/nvdebug"),
    ];
    for (case, path) in cases {
        let exe = Executable::new(path).expect("absolute");
        let result = exe.version(5 * SECOND, &CancellationToken::new()).await;
        assert!(matches!(result, Err(Error::Unavailable(_))), "{case}");
    }
}

#[tokio::test]
async fn an_unavailable_executable_keeps_the_os_error_as_its_cause() {
    let exe = Executable::new("/nonexistent/nvdebug").expect("absolute");

    let error = exe
        .version(5 * SECOND, &CancellationToken::new())
        .await
        .expect_err("missing");

    let cause = std::error::Error::source(&error)
        .and_then(|cause| cause.downcast_ref::<std::io::Error>())
        .expect("the io::Error, kept as the cause");
    assert_eq!(cause.kind(), std::io::ErrorKind::NotFound);
}

#[tokio::test]
async fn version_returns_nvdebug_s_first_line() {
    let version = fake().version(5 * SECOND, &CancellationToken::new()).await;

    assert_eq!(version.expect("ran"), "nvdebug 0.0.0 (fake)");
}

#[tokio::test]
async fn a_timeout_stops_the_run_and_reports_its_duration() {
    let (_dir, root) = real_tempdir();
    let timeout = Duration::from_millis(300);
    let started = std::time::Instant::now();

    let result = fake()
        .collect(
            &request(&root.join("out"), &["HOLD"], timeout),
            &CancellationToken::new(),
        )
        .await;

    assert!(matches!(result, Err(Error::Timeout(t)) if t == timeout));
    assert!(started.elapsed() < 4 * SECOND);
}

#[tokio::test]
async fn the_dut_file_is_private_and_named_for_this_process() {
    let (_dir, root) = real_tempdir();
    let out = root.join("out");
    let exe = fake();
    let request = request(&out, &["HOLD"], 10 * SECOND);
    let cancel = CancellationToken::new();

    let (result, dir) = tokio::join!(exe.collect(&request, &cancel), async {
        written(&out.join("ready")).await;
        let file = PathBuf::from(line_in(&out.join("dut_path")));
        let dir = dut_dir(&out);
        assert_eq!(mode(&file), 0o600);
        assert_eq!(mode(&dir), 0o700);
        let name = dir.file_name().and_then(|n| n.to_str()).expect("name");
        assert!(name.starts_with(&format!("nvdebug-{}-", std::process::id())));
        cancel.cancel();
        dir
    },);

    assert!(matches!(result, Err(Error::Cancelled)));
    assert!(!dir.exists());
}

#[tokio::test]
async fn no_run_returns_before_its_dut_directory_is_gone() {
    let cases = [
        ("finished", "1", 5 * SECOND, false),
        ("cancelled", "HOLD", 10 * SECOND, true),
        ("timed out", "HOLD", Duration::from_millis(500), false),
    ];
    for (case, behavior, timeout, cancel_when_ready) in cases {
        let (_dir, root) = real_tempdir();
        let out = root.join("out");
        let cancel = CancellationToken::new();

        tokio::join!(
            async {
                let _ = fake()
                    .collect(&request(&out, &[behavior], timeout), &cancel)
                    .await;
            },
            async {
                if cancel_when_ready {
                    written(&out.join("ready")).await;
                    cancel.cancel();
                }
            },
        );

        // Checked at once: the removal is awaited, not left to a task.
        assert!(!dut_dir(&out).exists(), "{case}");
    }
}

#[tokio::test]
async fn a_dropped_run_still_removes_its_dut_directory() {
    let (_dir, root) = real_tempdir();
    let out = root.join("out");
    let exe = fake();
    let request = request(&out, &["HOLD"], 10 * SECOND);
    let cancel = CancellationToken::new();
    let ready = out.join("ready");

    tokio::select! {
        result = exe.collect(&request, &cancel) => panic!("the fake holds: {result:?}"),
        () = written(&ready) => {}
    }

    // Dropped mid-run, the removal is left to the runtime.
    let dir = dut_dir(&out);
    let deadline = tokio::time::Instant::now() + 5 * SECOND;
    while dir.exists() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!dir.exists());
}

#[tokio::test]
async fn the_dut_file_maps_targets_by_name_with_the_host_bare_and_the_password_quoted() {
    let (_dir, root) = real_tempdir();
    let out = root.join("out");
    let target = |name: &str, host: &str, password: &str| {
        let host = BmcHost::parse(host).expect("a host");
        let bmc = Bmc::new(host, "admin".to_owned(), SecretString::from(password)).expect("bmc");
        Target::new(name, bmc, "GB200 NVL").expect("target")
    };
    let targets = vec![
        target("tray07", "10.1.2.30", "p@ss \"word\""),
        target("a", "fd00::1", "p"),
        target("c", "bmc-07.example.com", "p"),
    ];
    let ids = vec![CollectorId::new("0").expect("valid")];
    let request = CollectRequest::new(targets, ids, &out, 5 * SECOND).expect("valid");

    fake()
        .collect(&request, &CancellationToken::new())
        .await
        .expect("ran");

    let text = std::fs::read_to_string(out.join("dut.json")).expect("copied by the fake");
    let duts: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    assert_eq!(duts.as_object().map(serde_json::Map::len), Some(3));
    let tray = &duts["tray07"];
    assert_eq!(tray.get("name"), None);
    assert_eq!(tray["baseboard"], "GB200 NVL");
    assert_eq!(tray["bmc_ip"], "10.1.2.30");
    assert_eq!(tray.get("IP_NETWORK"), None);
    assert_eq!(tray["bmc_user"], "admin");
    assert_eq!(tray["bmc_pass"], "p@ss \"word\"");
    assert_eq!(duts["a"]["bmc_ip"], "fd00::1");
    assert_eq!(duts["a"]["IP_NETWORK"], "ipv6");
    assert_eq!(duts["c"]["bmc_ip"], "bmc-07.example.com");
    assert_eq!(duts["c"].get("IP_NETWORK"), None);
}

#[tokio::test]
async fn collect_and_dry_run_pass_the_committed_invocation() {
    let (_dir, root) = real_tempdir();
    let exe = fake();
    let args = |out: &Path| -> Vec<String> {
        let argv = std::fs::read_to_string(out.join("argv")).expect("recorded");
        argv.lines().map(str::to_owned).collect()
    };

    let out = root.join("collect");
    let collect = request(&out, &["0", "R9", "R1"], 600 * SECOND);
    exe.collect(&collect, &CancellationToken::new())
        .await
        .expect("ran");
    let dut = line_in(&out.join("dut_path"));
    let out_text = out.to_str().expect("utf-8");
    assert_eq!(
        args(&out),
        [
            "collect",
            "--dut-config",
            dut.as_str(),
            "--collector-id",
            "0 R9 R1",
            "--output",
            out_text,
            "--skip-html-reports",
            "--skip-zip",
            "--non-interactive",
        ]
    );

    let out = root.join("dry");
    let dry = request(&out, &["0"], 600 * SECOND);
    exe.dry_run(&dry, &CancellationToken::new())
        .await
        .expect("ran");
    assert_eq!(args(&out).last().map(String::as_str), Some("--dry-run"));
}

#[tokio::test]
async fn an_output_directory_that_holds_a_run_is_refused_before_the_spawn() {
    let (_dir, root) = real_tempdir();
    let out = root.join("out");
    std::fs::create_dir_all(out.join("nvdebug_logs_23_10_2026_21_43_00")).expect("created");

    let result = fake()
        .collect(
            &request(&out, &["0"], 5 * SECOND),
            &CancellationToken::new(),
        )
        .await;

    assert!(matches!(result, Err(Error::Invalid { .. })));
    assert!(!out.join("argv").exists(), "the fake ran");
}

#[tokio::test]
async fn a_run_concurrent_with_one_on_the_same_output_directory_is_refused() {
    let (_dir, root) = real_tempdir();
    let out = root.join("out");
    let exe = fake();
    let request = request(&out, &["HOLD"], 10 * SECOND);
    let cancel = CancellationToken::new();

    let (first, second) = tokio::join!(exe.collect(&request, &cancel), async {
        // The first run is past its claim once the fake is running.
        written(&out.join("ready")).await;
        let second = exe.collect(&request, &CancellationToken::new()).await;
        cancel.cancel();
        second
    });

    assert!(matches!(first, Err(Error::Cancelled)), "{first:?}");
    assert!(matches!(second, Err(Error::Invalid { .. })), "{second:?}");
}

#[tokio::test]
async fn collect_returns_the_status_uninterpreted_and_releases_the_output_directory() {
    // The fake leaves no run directory, so only a claim still held could
    // refuse the second run.
    let (_dir, root) = real_tempdir();
    let out = root.join("out");
    let request = request(&out, &["1"], 5 * SECOND);

    for run in ["first", "second"] {
        let collection = fake()
            .collect(&request, &CancellationToken::new())
            .await
            .unwrap_or_else(|error| panic!("{run}: {error}"));
        assert!(!collection.status.success(), "{run}");
        assert_eq!(collection.output_dir, out, "{run}");
        assert!(collection.started <= SystemTime::now(), "{run}");
    }
}
