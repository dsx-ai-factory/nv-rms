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
//! Exercises the `nvdebug_cli` API against a real nvdebug and, for `collect`,
//! a real BMC. Three subcommands in order of cost: `version` needs the
//! executable, `dry-run` needs a reachable configuration, `collect` needs the
//! rack. The BMC password comes from an environment variable, never argv.

use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use eyre::{WrapErr, eyre};
use nvdebug_cli::{
    Bmc, BmcHost, CancellationToken, CollectRequest, CollectorId, Executable, Target,
};

use secrecy::SecretString;

/// Environment variable that carries the BMC password.
const PASSWORD_ENV: &str = "NVDEBUG_PROBE_PASSWORD";

/// Exit code for a failure in the probe or the crate, as opposed to one
/// nvdebug reported. nvdebug uses 0, 1, 2, 3, and 130. GNU `timeout` uses
/// 125 for the same meaning, "the tool itself failed".
const PROBE_FAILURE: u8 = 125;

#[derive(Parser)]
#[command(
    name = "nvdebug-probe",
    about = "Runs nvdebug through the nvdebug_cli crate"
)]
struct Cli {
    /// Path of the nvdebug executable.
    #[arg(long)]
    nvdebug: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print `nvdebug --version`.
    Version {
        #[arg(long, default_value_t = 30)]
        timeout_secs: u64,
    },

    /// Validate a collection on nvdebug's side without touching the BMC.
    DryRun(CollectArgs),

    /// Collect from one BMC into the output directory.
    Collect(CollectArgs),
}

#[derive(Args)]
struct CollectArgs {
    /// Name of the target; becomes a directory under `--out`.
    #[arg(long, default_value = "probe")]
    name: String,

    /// BMC IP address or DNS name, without brackets, port or IPv6 zone id.
    #[arg(long)]
    bmc: BmcHost,

    /// BMC user. The password is read from NVDEBUG_PROBE_PASSWORD.
    #[arg(long)]
    user: String,

    /// nvdebug baseboard name, for example "GB200 NVL".
    #[arg(long)]
    baseboard: String,

    /// Collector ids, comma-separated, for example R8,R9,R1.
    #[arg(long, value_delimiter = ',')]
    collectors: Vec<String>,

    /// Output directory, absolute; nvdebug creates its own subdirectory
    /// inside.
    #[arg(long)]
    out: PathBuf,

    #[arg(long, default_value_t = 600)]
    timeout_secs: u64,
}

/// Parses the arguments, takes the password out of the environment, runs
/// the command on a runtime, and maps the outcome to an exit code.
fn main() -> ExitCode {
    // The crate warns when `--version` fails or a run leaves something
    // behind, and under RUST_LOG=info it says when a collection starts and
    // how it ended.
    // Those lines go to stderr, in colour only when stderr is a terminal,
    // so a redirected run reads clean in a file.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();

    // clap exits 2 on a usage error, the code nvdebug uses for its own, so
    // the probe's usage errors are routed to its failure code. Help and
    // version requests keep clap's exit 0.
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) if error.use_stderr() => {
            error.print().ok();
            return ExitCode::from(PROBE_FAILURE);
        }
        Err(help) => help.exit(),
    };

    // nvdebug never sees this variable: the crate clears nvdebug's
    // environment and passes on only a short allow-list.
    let password = std::env::var(PASSWORD_ENV).ok().map(SecretString::from);

    let result = tokio::runtime::Runtime::new()
        .wrap_err("could not start the runtime")
        .and_then(|runtime| runtime.block_on(run(cli, password)));
    match result {
        Ok(code) => code,
        Err(error) => {
            // `{:#}` prints the source chain: the crate's errors keep their
            // cause there, not in their own text.
            eprintln!("nvdebug-probe: {error:#}");
            ExitCode::from(PROBE_FAILURE)
        }
    }
}

/// Runs the chosen subcommand against the executable the arguments name.
async fn run(cli: Cli, password: Option<SecretString>) -> eyre::Result<ExitCode> {
    let exe = Executable::new(cli.nvdebug)?;

    // A probe killed with SIGKILL leaves its DUT directory, with the
    // password, in the temp root. The next probe removes it.
    nvdebug_cli::sweep_stale_dut_dirs().await;

    // nvdebug runs in a process group of its own, out of reach of the
    // terminal's Ctrl-C, so the probe passes Ctrl-C on as a cancel.
    let cancel = CancellationToken::new();
    let on_ctrl_c = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            on_ctrl_c.cancel();
        }
    });

    match cli.command {
        Command::Version { timeout_secs } => {
            let version = exe
                .version(Duration::from_secs(timeout_secs), &cancel)
                .await?;
            println!("{version}");
            Ok(ExitCode::SUCCESS)
        }
        Command::DryRun(args) => {
            let request = request(args, password)?;
            Ok(report(exe.dry_run(&request, &cancel).await?))
        }
        Command::Collect(args) => {
            let request = request(args, password)?;
            Ok(report(exe.collect(&request, &cancel).await?))
        }
    }
}

/// Builds the one-target request the probe collects with. The password
/// must have been in the environment at start.
fn request(args: CollectArgs, password: Option<SecretString>) -> eyre::Result<CollectRequest> {
    let password = password.ok_or_else(|| eyre!("set {PASSWORD_ENV} to the BMC password"))?;
    let bmc = Bmc::new(args.bmc, args.user, password)?;
    let target = Target::new(args.name, bmc, args.baseboard)?;
    let collectors = args
        .collectors
        .into_iter()
        .map(CollectorId::new)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CollectRequest::new(
        vec![target],
        collectors,
        args.out,
        Duration::from_secs(args.timeout_secs),
    )?)
}

/// Prints what came back and exits with nvdebug's own code, so a shell can
/// read it the way it would read nvdebug directly. A signal death becomes
/// 128 plus the signal, the shell's own convention, so a kill does not read
/// as nvdebug's runtime failure code 1.
fn report(collection: nvdebug_cli::Collection) -> ExitCode {
    println!("{}", collection.status);
    println!("output dir: {}", collection.output_dir.display());
    if let Ok(since_epoch) = collection.started.duration_since(std::time::UNIX_EPOCH) {
        println!("started: {} s since epoch", since_epoch.as_secs());
    }

    let code = match (collection.status.code(), collection.status.signal()) {
        (Some(code), _) => code,
        (None, Some(signal)) => 128 + signal,
        (None, None) => 1,
    };
    ExitCode::from(u8::try_from(code).unwrap_or(1))
}
