# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
FastAPI add-on module that exposes NVSSVT as an HTTP service.

This is intentionally kept separate from the CLI (src/nvssvt.py): it launches
NVSSVT as a subprocess (the same way NVSSVT launches itself for multi-DUT
runs, see create_nvssvt_sub() in src/nvssvt.py) and tracks the run by reading
the same JSON artifacts NVSSVT already writes to its log directory.

It always launches the standalone NVSSVT PyInstaller binary from an NVSSVT
release bundle (see docker/nvssvt/Dockerfile), never the CLI source directly -
set NVSSVT_BIN to that binary's path. The binary runs with cwd set to this
file's parent.parent, which must hold the bundle's config files.

The API has no authentication, so it listens on loopback only. RMS runs in
the same pod and reaches it on 127.0.0.1.

Run with:
    NVSSVT_BIN=/app/nvssvt uvicorn nvssvt_server:app --host 127.0.0.1 --port 8000
or:
    NVSSVT_BIN=/app/nvssvt python nvssvt_server.py
"""

import json
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import yaml
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

REPO_ROOT = Path(__file__).resolve().parent.parent
NVSSVT_BIN = os.environ.get("NVSSVT_BIN")
RUNS_ROOT = REPO_ROOT / "api_runs"

# Number of most-recent run directories to keep under RUNS_ROOT; older ones
# are deleted on each new run request to avoid unbounded disk usage.
DEFAULT_MAX_RETAINED_RUNS = 12
MAX_RETAINED_RUNS_ENV = "NVSSVT_MAX_RETAINED_RUNS"

# Run directories modified more recently than this are never deleted, even
# beyond the retention limit, so RMS has time to poll a run's final result.
DEFAULT_MIN_RUN_AGE_SECONDS = 3600
MIN_RUN_AGE_ENV = "NVSSVT_MIN_RUN_AGE_SECONDS"

NODE_SERVICE_STATUS_FILE = "Node_service_status.json"
RESULT_SUMMARY_FILENAMES = (
    "NVSSVTSummaryReport.json",
    "NVSSVTMultiNodeSummaryReport.json",
    "NVFWPunishMultiNodeSummaryReport.json",
)
TAIL_BYTES = 4000

app = FastAPI(title="NVSSVT Launcher API", version="1.0")

RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")

_runs: Dict[str, dict] = {}
# run_ids claimed by an in-flight launch_run() that hasn't registered in _runs yet.
_reserved_run_ids: Set[str] = set()
_runs_lock = threading.Lock()


class LaunchRequest(BaseModel):
    """
    Attributes needed to launch an NVSSVT run. Fields map directly to the
    NVSSVT CLI flags (see src/nvssvt_cli_help.py); anything left unset falls
    back to the NVSSVT CLI's own defaults (e.g. config.yaml/dut_config.yaml
    in the repo root).
    """

    run_id: Optional[str] = None
    bmc_ip: Optional[str] = None
    bmc_user: Optional[str] = None
    bmc_pwd: Optional[str] = None
    host_ip: Optional[str] = None
    host_user: Optional[str] = None
    host_pwd: Optional[str] = None
    quallevel: List[str] = Field(default_factory=list)
    mgmtsuites: List[str] = Field(default_factory=list)
    servertype: Optional[str] = None
    boardtype: Optional[str] = None
    dcgmport: Optional[str] = None
    config: Optional[str] = None
    dutconfig: Optional[str] = None
    # Raw YAML text (not a path, unlike the other fields here) written to
    # config_<run_id>.yaml in the run directory; that path is what's passed
    # to --config-override.
    config_override: Optional[str] = None
    logdir: Optional[str] = None
    useroemconfig: Optional[str] = None
    ignorequallevelteststorun: List[str] = Field(default_factory=list)
    catalogtesttoexecute: List[str] = Field(default_factory=list)
    verbose: bool = False
    local: bool = False
    nologsanitization: bool = False
    stop_on_preflight_failure: bool = False
    allow_preflight_failures: bool = False
    allow_security_failures: bool = False
    auto_remediate: bool = False
    fast_start: bool = False
    clear_bmc_logs: bool = False
    ignoreconfigversion: bool = False


class LaunchResponse(BaseModel):
    run_id: str
    pid: int
    log_dir: str
    started_at: str


def _build_argv(req: LaunchRequest, run_dir: Path) -> List[str]:
    argv = [NVSSVT_BIN]

    def add(flag: str, value: Optional[str]) -> None:
        if value:
            argv.extend([flag, str(value)])

    add("--bmcip", req.bmc_ip)
    add("--bmcuser", req.bmc_user)
    add("--bmcpwd", req.bmc_pwd)
    add("--hostip", req.host_ip)
    add("--hostuser", req.host_user)
    add("--hostpwd", req.host_pwd)
    add("--servertype", req.servertype)
    add("--boardtype", req.boardtype)
    add("--dcgmport", req.dcgmport)
    add("--config", req.config)
    add("--dutconfig", req.dutconfig)
    if req.config_override:
        override_path = run_dir / f"config_{run_dir.name}.yaml"
        override_path.write_text(req.config_override, encoding="utf-8")
        add("--config-override", str(override_path))
    add("--useroemconfig", req.useroemconfig)

    if req.quallevel:
        argv += ["--quallevel", *req.quallevel]
    if req.mgmtsuites:
        argv += ["--mgmtsuites", *req.mgmtsuites]
    if req.ignorequallevelteststorun:
        argv += ["--ignorequallevelteststorun", *req.ignorequallevelteststorun]
    if req.catalogtesttoexecute:
        argv += ["--catalogtesttoexecute", *req.catalogtesttoexecute]

    for flag, enabled in (
        ("--verbose", req.verbose),
        ("--local", req.local),
        ("--nologsanitization", req.nologsanitization),
        ("--stop-on-preflight-failure", req.stop_on_preflight_failure),
        ("--allow-preflight-failures", req.allow_preflight_failures),
        ("--allow-security-failures", req.allow_security_failures),
        ("--auto-remediate", req.auto_remediate),
        ("--fast-start", req.fast_start),
        ("--clear-bmc-logs", req.clear_bmc_logs),
        ("--ignoreconfigversion", req.ignoreconfigversion),
    ):
        if enabled:
            argv.append(flag)

    # Keep results as a browsable folder (not a .zip) so the status endpoint
    # can read the JSON artifacts while, and after, the run is in progress.
    argv += ["--logdir", str(run_dir), "--nologcompression"]
    return argv


def _max_retained_runs() -> int:
    try:
        return max(0, int(os.environ.get(MAX_RETAINED_RUNS_ENV, DEFAULT_MAX_RETAINED_RUNS)))
    except ValueError:
        return DEFAULT_MAX_RETAINED_RUNS


def _min_run_age_seconds() -> float:
    try:
        return max(0.0, float(os.environ.get(MIN_RUN_AGE_ENV, DEFAULT_MIN_RUN_AGE_SECONDS)))
    except ValueError:
        return float(DEFAULT_MIN_RUN_AGE_SECONDS)


def _mtime(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except OSError:
        # Another request deleted it after it was listed.
        return None


def _cleanup_old_runs() -> None:
    """
    Delete the oldest run directories under RUNS_ROOT beyond the retention
    limit (NVSSVT_MAX_RETAINED_RUNS, default DEFAULT_MAX_RETAINED_RUNS) so
    disk usage doesn't grow unbounded. Runs that are reserved or still in
    progress are kept regardless of age/position, as are directories modified
    within the last NVSSVT_MIN_RUN_AGE_SECONDS.
    """
    retain = _max_retained_runs()
    min_age = _min_run_age_seconds()
    if not RUNS_ROOT.is_dir():
        return

    dated_dirs = []
    for run_dir in RUNS_ROOT.iterdir():
        mtime = _mtime(run_dir)
        if mtime is not None and run_dir.is_dir():
            dated_dirs.append((mtime, run_dir))
    dated_dirs.sort(key=lambda dated: dated[0], reverse=True)

    now = time.time()
    for mtime, run_dir in dated_dirs[retain:]:
        if now - mtime < min_age:
            continue
        # Check and unregister under the lock so a launch can't reserve or
        # register this run_id in between.
        with _runs_lock:
            record = _runs.get(run_dir.name)
            if run_dir.name in _reserved_run_ids or (record and record["process"].poll() is None):
                continue
            _runs.pop(run_dir.name, None)
        shutil.rmtree(run_dir, ignore_errors=True)


def _tail(path: Path, size: int = TAIL_BYTES) -> Optional[str]:
    if not path.exists():
        return None
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            file_size = f.tell()
            f.seek(max(0, file_size - size))
            return f.read().decode("utf-8", errors="replace")
    except OSError:
        return None


def _find_result_dir(run_dir: Path) -> Optional[Path]:
    candidates = sorted(run_dir.glob("NVSSVT_Logs_*"))
    return candidates[0] if candidates else None


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        # Node_service_status.json can be read mid-write by NVSSVT; treat a
        # transient parse failure as "not available yet" rather than an error.
        return None


@app.post("/runs", response_model=LaunchResponse)
def launch_run(req: LaunchRequest) -> LaunchResponse:
    """
    Launch an NVSSVT process for the given DUT/config attributes.
    """
    if not NVSSVT_BIN:
        raise HTTPException(status_code=500, detail="NVSSVT_BIN environment variable is not set")
    if not Path(NVSSVT_BIN).exists():
        raise HTTPException(status_code=500, detail=f"NVSSVT binary not found at {NVSSVT_BIN}")
    if req.config_override:
        try:
            yaml.safe_load(req.config_override)
        except yaml.YAMLError as exc:
            raise HTTPException(status_code=400, detail=f"config_override is not valid YAML: {exc}") from exc

    _cleanup_old_runs()

    run_id = req.run_id or uuid.uuid4().hex[:12]
    # run_id becomes a directory name under RUNS_ROOT, so reject anything that
    # could escape it (path separators, "..", absolute paths).
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise HTTPException(
            status_code=400,
            detail="run_id must be 1-64 characters of [A-Za-z0-9._-] and start with a letter or digit",
        )

    # Reserve the run_id under the lock so concurrent requests can't both pass
    # the duplicate check before either registers its run.
    with _runs_lock:
        if run_id in _runs or run_id in _reserved_run_ids:
            raise HTTPException(status_code=409, detail=f"run_id '{run_id}' already exists")
        _reserved_run_ids.add(run_id)

    run_dir = RUNS_ROOT / run_id
    try:
        RUNS_ROOT.mkdir(parents=True, exist_ok=True)
        # exist_ok=False: a directory left by an earlier server process (whose
        # _runs registry is gone) must not be reused or overwritten.
        run_dir.mkdir()
    except FileExistsError as exc:
        with _runs_lock:
            _reserved_run_ids.discard(run_id)
        raise HTTPException(status_code=409, detail=f"run_id '{run_id}' already exists") from exc
    except BaseException:
        with _runs_lock:
            _reserved_run_ids.discard(run_id)
        raise

    try:
        argv = _build_argv(req, run_dir)
        stdout_path = run_dir / "server_stdout.log"
        stderr_path = run_dir / "server_stderr.log"

        with open(stdout_path, "w", encoding="utf-8") as stdout_f, open(
            stderr_path, "w", encoding="utf-8"
        ) as stderr_f:
            process = subprocess.Popen(argv, cwd=str(REPO_ROOT), stdout=stdout_f, stderr=stderr_f)
    except BaseException:
        # Only this request created run_dir, so it is safe to remove.
        with _runs_lock:
            _reserved_run_ids.discard(run_id)
        shutil.rmtree(run_dir, ignore_errors=True)
        raise

    started_at = datetime.now()
    with _runs_lock:
        _reserved_run_ids.discard(run_id)
        _runs[run_id] = {
            "process": process,
            "run_dir": run_dir,
            "stdout_path": stdout_path,
            "stderr_path": stderr_path,
            "argv": argv,
            "started_at": started_at,
        }

    return LaunchResponse(
        run_id=run_id,
        pid=process.pid,
        log_dir=str(run_dir),
        started_at=started_at.isoformat(),
    )


@app.get("/runs/{run_id}")
def get_run_status(run_id: str) -> dict:
    """
    Track progress of a previously launched NVSSVT run and return results
    once available.
    """
    with _runs_lock:
        record = _runs.get(run_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Unknown run_id '{run_id}'")

    process = record["process"]
    return_code = process.poll()
    run_dir = record["run_dir"]
    result_dir = _find_result_dir(run_dir)

    if return_code is None:
        state = "running" if result_dir else "starting"
    else:
        state = "completed" if return_code == 0 else "failed"

    duts = {}
    overall_progress = None
    if result_dir:
        service_status = _read_json(result_dir / NODE_SERVICE_STATUS_FILE) or {}
        duts = service_status
        progresses = [
            info["OVERALL_NVSSVT_PROGRESS"]
            for info in service_status.values()
            if isinstance(info, dict) and isinstance(info.get("OVERALL_NVSSVT_PROGRESS"), (int, float))
        ]
        if progresses:
            overall_progress = sum(progresses) / len(progresses)

    summary = None
    if return_code is not None and result_dir:
        for filename in RESULT_SUMMARY_FILENAMES:
            summary_path = result_dir / filename
            if summary_path.exists():
                summary = _read_json(summary_path)
                break

    error = None
    if return_code not in (None, 0):
        error = _tail(record["stderr_path"]) or _tail(record["stdout_path"])

    return {
        "run_id": run_id,
        "pid": process.pid,
        "state": state,
        "return_code": return_code,
        "started_at": record["started_at"].isoformat(),
        "log_dir": str(result_dir) if result_dir else str(run_dir),
        "overall_progress": overall_progress,
        "duts": duts,
        "summary": summary,
        "error": error,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
