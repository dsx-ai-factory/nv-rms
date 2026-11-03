# nvssvt

Launches and tracks NVSSVT scans through NVSSVT's own launcher API
(`nvssvt_server.py`). `NvssvtRunner::launch` posts a scan and returns
immediately with the launcher's run handle; `NvssvtRunner::status` polls that
run until it finishes, at which point the parsed summary report is available
too.

NVSSVT runs as a second container in the same pod as the embedding server, so
the default target is `http://127.0.0.1:8000` and no configuration is
required; host and port are overridable for development against an instance
running elsewhere. The launcher owns the NVSSVT subprocess and its log
directory, so `pid` and `log_dir` describe the launcher's filesystem, not the
caller's -- they are reported verbatim rather than interpreted.

This crate is deliberately free of any gRPC service implementation or
job-tracking policy, kept as a standalone crate so it can be embedded into
any async Rust server without pulling in the rest of this workspace.

## Endpoints

| Call | Endpoint |
| --- | --- |
| `NvssvtRunner::launch` | `POST /runs` |
| `NvssvtRunner::status` | `GET /runs/{run_id}` |

`launch` takes an optional `run_id`: pass the caller's own identifier for the
scan (`rackmanagementservice` passes its job id) and both sides name the run
identically, so neither keeps an id mapping. The launcher generates an id
when it is `None`, and either way echoes the id it used on
`LaunchOutcome::run_id` -- that echo is what to poll.

A `404` maps to `NvssvtError::RunNotFound` (gRPC `NOT_FOUND`) and a `409`
(run id already in use) to `NvssvtError::RunAlreadyExists` (gRPC
`ALREADY_EXISTS`); an unreachable launcher maps to `NvssvtError::Transport`
(gRPC `UNAVAILABLE`), which callers can retry -- a sidecar that is still
starting looks exactly like this.

## Embedding

`rackmanagementservice` drives `NvssvtRunner` from its own async job
framework (`JobType::SystemValidation` in `JobTracker`) and exposes it over
`RackManagerV2.StartSystemValidation` (defined in `librms`, not in this
crate -- see `crates/rackmanagementservice/src/api/grpc/nvssvt_handlers.rs`).
The handler builds a runner per request from `NvssvtConfig::default()`, i.e.
the in-pod sidecar at `127.0.0.1:8000`; RMS exposes no config to change it.

`StartScanFields` carries only the parameters RMS drives -- DUT credentials
and the qualification level -- named after `nvssvt_server.py`'s
`LaunchRequest` JSON keys (themselves a mapping of NVSSVT's CLI flags, see
`src/nvssvt_cli_help.py` in the NVSSVT repo). Fields left unset are omitted
from the request so NVSSVT applies its own CLI defaults; add a field here and
to `LaunchRequestBody` when a new knob needs driving. `ScanStatus.duts`/`summary`
are raw `serde_json::Value` passthroughs rather than modeled structs, since
their shape varies by platform and NVSSVT version.

## Tests

`tests/runner.rs` exercises the runner against a stubbed launcher
(`wiremock`), so the suite is hermetic: no NVSSVT install, no DUT, no
subprocess. End-to-end coverage through the RMS job framework lives in
`crates/rackmanagementservice/src/api/grpc/nvssvt_handlers.rs`.
