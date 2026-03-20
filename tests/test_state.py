import gzip
import hashlib
import http.server
import json
import os
import shutil
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from api_surface import (
    ABORTED_BY_KEY,
    ABORTED_JOBS_KEY,
    ABORTED_STATE,
    ABORT_JOBS_ROUTE,
    DONE_STATE,
    ERRORED_STATE,
    ITEMS_KEY,
    JOB_ABORT_KIND,
    JOB_ID_KEY,
    JOB_NOT_FOUND_CODE,
    JOB_PRUNE_KIND,
    JOB_ROUTE,
    JOBS_ROUTE,
    KIND_KEY,
    MATCHED_JOBS_KEY,
    MISSING_JOB_FILTERS_CODE,
    QUEUED_STATE,
    RECORDS_BODY_KEY,
    REMOVED_JOBS_KEY,
    RUNNING_STATE,
    STATE_KEY,
    STORE_RECORDS_ROUTE,
)

REFERENCE_BASE = "http://127.0.0.1:9108"
CONTROL_BASE = "http://127.0.0.1:19110"
CANDIDATE_BASE = "http://127.0.0.1:8108"
CANDIDATE_DIR = Path("/workspace/candidate")
PROBE_LAB = Path("/workspace/probe_lab")
ASYNC_STATUS_LOG_PATH = Path("/logs/verifier/async_statuses.log")
IMMUTABLE_HASHES = {"README.md": "2f9bc972d5066e282d1ffeac6a24e36e37c4bc3008c2f37fbc2898e47edd62b0", "clear_reference_hookbox.sh": "137536bb0d4c3e510e502f7a0dfe168c6075a5ed66d77e1532453c1b316257b8", "reset_reference.sh": "e21547c33fda1f129af72bc473ec26bf7f7c445a76e52abe410eddee7a201d7f", "restart_reference.sh": "18287a857f2304223e803983999d68e046031b98ffdc3bd1f7e7d37a3e789535", "wait_reference.sh": "1a1d5dfe45556dc81196409cf130d8bfe10f7c8852f9cab2e674e147d0ebac19"}
WEBHOOK_AUTH = "Bearer hidden-verifier-auth"
EXPECTED_ENQUEUE_STATUS = 202
ACCEPTED_ENQUEUE_STATUSES = {200, EXPECTED_ENQUEUE_STATUS}
ENQUEUE_STATUS_PENALTY = 0.05


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def request(method: str, url: str, **kwargs):
    return requests.request(method, url, timeout=kwargs.pop("timeout", 5), **kwargs)


def record_async_status(status_code: int) -> None:
    ASYNC_STATUS_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ASYNC_STATUS_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"{status_code}\n")


def wait_http_ok(url: str, timeout: float = 20.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = request("GET", url, timeout=0.5)
            if r.ok:
                return r
            last = f"status={r.status_code}"
        except Exception as exc:
            last = repr(exc)
        time.sleep(0.1)
    raise AssertionError(f"service at {url} did not become healthy: {last}")


def stop_reference() -> None:
    r = request("POST", f"{CONTROL_BASE}/control/stop-reference")
    assert r.ok, r.text


def assert_reference_healthy() -> None:
    r = request("GET", f"{REFERENCE_BASE}/health")
    assert r.status_code == 200, r.text
    assert r.json() == {"status": "available"}


class HookState:
    def __init__(self, block_first: bool = False, fail_first: int = 0):
        self.block_first = block_first
        self.fail_first = fail_first
        self.release_event = threading.Event()
        self.first_arrived = threading.Event()
        self.lock = threading.Lock()
        self.deliveries: List[Dict] = []
        self.count = 0


class HookHandler(http.server.BaseHTTPRequestHandler):
    state: HookState = None  # type: ignore

    def do_POST(self):
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length)
        headers = {k.lower(): v for k, v in self.headers.items()}

        with self.state.lock:
            index = self.state.count
            self.state.count += 1
            should_block = self.state.block_first and index == 0
            should_fail = index < self.state.fail_first

        if should_block:
            self.state.first_arrived.set()
            released = self.state.release_event.wait(timeout=30)
            if not released:
                self.send_response(504)
                self.end_headers()
                return

        if should_fail:
            self.send_response(500)
            self.end_headers()
            return

        decoded_text = None
        lines = []
        try:
            decoded_text = gzip.decompress(body).decode("utf-8")
            for line in decoded_text.splitlines():
                if line.strip():
                    lines.append(json.loads(line))
        except Exception:
            decoded_text = None

        with self.state.lock:
            self.state.deliveries.append({
                "headers": headers,
                "raw": body,
                "decoded_text": decoded_text,
                "lines": lines,
            })

        self.send_response(202)
        self.end_headers()

    def log_message(self, format, *args):
        return


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


class HookServer:
    def __init__(self, block_first: bool = False, fail_first: int = 0):
        self.state = HookState(block_first=block_first, fail_first=fail_first)
        handler = type("BoundHookHandler", (HookHandler,), {"state": self.state})
        self.httpd = ThreadedTCPServer(("127.0.0.1", 18109), handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


class CandidateProcess:
    def __init__(self, state_dir: Path, webhook_url: Optional[str] = None, webhook_auth: Optional[str] = None, processing_steps: int = 16, processing_step_seconds: float = 0.05):
        self.state_dir = state_dir
        self.webhook_url = webhook_url
        self.webhook_auth = webhook_auth
        self.processing_steps = processing_steps
        self.processing_step_seconds = processing_step_seconds
        self.proc: Optional[subprocess.Popen] = None

    def start(self):
        env = os.environ.copy()
        env["ENGINE_DATA_DIR"] = str(self.state_dir)
        env["ENGINE_PROCESSING_STEPS"] = str(self.processing_steps)
        env["ENGINE_PROCESSING_STEP_SECONDS"] = str(self.processing_step_seconds)
        if self.webhook_url:
            env["ENGINE_TASK_WEBHOOK_URL"] = self.webhook_url
        else:
            env.pop("ENGINE_TASK_WEBHOOK_URL", None)
        if self.webhook_auth:
            env["ENGINE_TASK_WEBHOOK_AUTHORIZATION_HEADER"] = self.webhook_auth
        else:
            env.pop("ENGINE_TASK_WEBHOOK_AUTHORIZATION_HEADER", None)
        self.proc = subprocess.Popen(["bash", str(CANDIDATE_DIR / "run.sh")], cwd=str(CANDIDATE_DIR), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_http_ok(f"{CANDIDATE_BASE}/health", timeout=20)
        return self

    def stop(self):
        if self.proc is None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        self.proc = None


def enqueue_docs(index_uid: str, docs: List[Dict]):
    r = request("POST", f"{CANDIDATE_BASE}{STORE_RECORDS_ROUTE.format(index_uid=index_uid)}", json={RECORDS_BODY_KEY: docs})
    assert r.status_code in ACCEPTED_ENQUEUE_STATUSES, r.text
    record_async_status(r.status_code)
    return r.json()[JOB_ID_KEY]


def get_task(uid: int):
    r = request("GET", f"{CANDIDATE_BASE}{JOB_ROUTE.format(task_uid=uid)}")
    return r


def list_tasks(**params):
    r = request("GET", f"{CANDIDATE_BASE}{JOBS_ROUTE}", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def wait_task(uid: int, target_statuses=None, timeout: float = 20.0):
    target_statuses = set(target_statuses or [DONE_STATE, ERRORED_STATE, ABORTED_STATE])
    deadline = time.time() + timeout
    latest = None
    while time.time() < deadline:
        r = get_task(uid)
        assert r.status_code in (200, 404), r.text
        if r.status_code == 200:
            latest = r.json()
            if latest[STATE_KEY] in target_statuses:
                return latest
        time.sleep(0.05)
    raise AssertionError(f"task {uid} did not reach {target_statuses}: latest={latest}")


def list_docs(index_uid: str):
    r = request("GET", f"{CANDIDATE_BASE}{STORE_RECORDS_ROUTE.format(index_uid=index_uid)}")
    assert r.status_code == 200, r.text
    return r.json()[ITEMS_KEY]


def check_probe_lab_integrity():
    failures = []
    for name, expected in IMMUTABLE_HASHES.items():
        actual = sha256_file(PROBE_LAB / name)
        if actual != expected:
            failures.append(f"{name} hash mismatch")
    assert not failures, "; ".join(failures)
    assert_reference_healthy()


def case_batching_and_hook_payload():
    hook = HookServer().start()
    try:
        with tempfile.TemporaryDirectory() as td:
            cp = CandidateProcess(Path(td), webhook_url="http://127.0.0.1:18109/hook", webhook_auth=WEBHOOK_AUTH, processing_steps=8, processing_step_seconds=0.03).start()
            try:
                a0 = enqueue_docs("alpha", [{"id": 1, "title": "a0"}])
                a1 = enqueue_docs("alpha", [{"id": 2, "title": "a1"}])
                b0 = enqueue_docs("beta", [{"id": 10, "title": "b0"}])
                ta0 = wait_task(a0)
                ta1 = wait_task(a1)
                tb0 = wait_task(b0)
                assert ta0[STATE_KEY] == ta1[STATE_KEY] == tb0[STATE_KEY] == DONE_STATE
                assert ta0["startedAt"] == ta1["startedAt"]
                assert ta0["endedAt"] == ta1["endedAt"]
                assert ta0["elapsed"] == ta1["elapsed"]
                assert not (tb0["startedAt"] == ta0["startedAt"] and tb0["endedAt"] == ta0["endedAt"])
                assert [d["id"] for d in list_docs("alpha")] == [1, 2]
                assert [d["id"] for d in list_docs("beta")] == [10]
                deadline = time.time() + 10
                while time.time() < deadline:
                    if len(hook.state.deliveries) >= 2:
                        break
                    time.sleep(0.05)
                assert len(hook.state.deliveries) == 2
                assert hook.state.deliveries[0]["headers"].get("authorization") == WEBHOOK_AUTH
                assert hook.state.deliveries[0]["headers"].get("content-encoding") == "gzip"
                assert len(hook.state.deliveries[0]["lines"]) == 2
                assert len(hook.state.deliveries[1]["lines"]) == 1
            finally:
                cp.stop()
    finally:
        hook.stop()


def case_cancel_interrupts_batch():
    with tempfile.TemporaryDirectory() as td:
        cp = CandidateProcess(Path(td), processing_steps=20, processing_step_seconds=0.05).start()
        try:
            t0 = enqueue_docs("ix", [{"id": 1, "title": "keep"}])
            t1 = enqueue_docs("ix", [{"id": 2, "title": "cancel-me"}])
            t2 = enqueue_docs("ix", [{"id": 3, "title": "also-cancel"}])
            time.sleep(0.12)
            r = request("POST", f"{CANDIDATE_BASE}{ABORT_JOBS_ROUTE}", params={"ids": f"{t1},{t2}"})
            assert r.status_code in ACCEPTED_ENQUEUE_STATUSES, r.text
            record_async_status(r.status_code)
            cancel_uid = r.json()[JOB_ID_KEY]
            cancel_task = wait_task(cancel_uid)
            after_t0 = wait_task(t0)
            after_t1 = wait_task(t1)
            after_t2 = wait_task(t2)
            assert cancel_task[KIND_KEY] == JOB_ABORT_KIND
            assert cancel_task[STATE_KEY] == DONE_STATE
            assert cancel_task["meta"][MATCHED_JOBS_KEY] >= 2
            assert cancel_task["meta"][ABORTED_JOBS_KEY] >= 2
            assert after_t0[STATE_KEY] == DONE_STATE
            assert after_t1[STATE_KEY] == ABORTED_STATE
            assert after_t2[STATE_KEY] == ABORTED_STATE
            assert after_t1[ABORTED_BY_KEY] == cancel_uid
            assert after_t2[ABORTED_BY_KEY] == cancel_uid
            docs = list_docs("ix")
            assert [d["id"] for d in docs] == [1]
            filtered = list_tasks(states=ABORTED_STATE, abortedBy=str(cancel_uid))
            canceled_ids = sorted(t[JOB_ID_KEY] for t in filtered[ITEMS_KEY])
            assert t1 in canceled_ids and t2 in canceled_ids
        finally:
            cp.stop()


def case_task_deletion_filters():
    with tempfile.TemporaryDirectory() as td:
        cp = CandidateProcess(Path(td), processing_steps=20, processing_step_seconds=0.05).start()
        try:
            t0 = enqueue_docs("delix", [{"id": 1, "title": "x"}])
            t1 = enqueue_docs("delix", [{"id": 2, "title": "y"}])
            time.sleep(0.1)
            r = request("POST", f"{CANDIDATE_BASE}{ABORT_JOBS_ROUTE}", params={"ids": str(t1)})
            assert r.status_code in ACCEPTED_ENQUEUE_STATUSES, r.text
            record_async_status(r.status_code)
            cancel_uid = r.json()[JOB_ID_KEY]
            wait_task(cancel_uid)
            wait_task(t0)
            wait_task(t1)
            missing = request("DELETE", f"{CANDIDATE_BASE}{JOBS_ROUTE}")
            assert missing.status_code == 400, missing.text
            assert missing.json()["code"] == MISSING_JOB_FILTERS_CODE
            delete = request("DELETE", f"{CANDIDATE_BASE}{JOBS_ROUTE}", params={"states": ABORTED_STATE})
            assert delete.status_code in ACCEPTED_ENQUEUE_STATUSES, delete.text
            record_async_status(delete.status_code)
            delete_uid = delete.json()[JOB_ID_KEY]
            delete_task = wait_task(delete_uid)
            assert delete_task[KIND_KEY] == JOB_PRUNE_KIND
            assert delete_task["meta"][MATCHED_JOBS_KEY] >= 1
            assert delete_task["meta"][REMOVED_JOBS_KEY] >= 1
            r404 = get_task(t1)
            assert r404.status_code == 404, r404.text
            assert r404.json()["code"] == JOB_NOT_FOUND_CODE
        finally:
            cp.stop()


def case_webhook_blocks_scheduler_and_logs_failures():
    hook = HookServer(block_first=True, fail_first=1).start()
    try:
        with tempfile.TemporaryDirectory() as td:
            cp = CandidateProcess(Path(td), webhook_url="http://127.0.0.1:18109/hook", webhook_auth=WEBHOOK_AUTH, processing_steps=6, processing_step_seconds=0.03).start()
            try:
                t0 = enqueue_docs("wh", [{"id": 1, "title": "a"}])
                t1 = enqueue_docs("wh", [{"id": 2, "title": "b"}])
                first = wait_task(t0)
                second = wait_task(t1)
                assert first[STATE_KEY] == second[STATE_KEY] == DONE_STATE

                t2 = enqueue_docs("wh", [{"id": 3, "title": "c"}])
                time.sleep(0.15)
                assert hook.state.first_arrived.wait(timeout=10), "first hook request never arrived"
                t3 = enqueue_docs("wh", [{"id": 4, "title": "d"}])
                time.sleep(0.25)
                mid = wait_task(t3, target_statuses=[QUEUED_STATE, RUNNING_STATE, DONE_STATE], timeout=2)
                assert mid[STATE_KEY] in [QUEUED_STATE, RUNNING_STATE]
                hook.state.release_event.set()
                after_t2 = wait_task(t2)
                after_t3 = wait_task(t3)
                assert after_t2[STATE_KEY] == after_t3[STATE_KEY] == DONE_STATE
                log_path = Path(td) / "scheduler.log"
                deadline = time.time() + 10
                while time.time() < deadline and not log_path.exists():
                    time.sleep(0.05)
                assert log_path.exists(), "expected scheduler log after webhook failure"
                log_text = log_path.read_text(encoding="utf-8")
                assert "webhook_error" in log_text
                deadline = time.time() + 10
                while time.time() < deadline:
                    if len(hook.state.deliveries) >= 1:
                        break
                    time.sleep(0.05)
                assert len(hook.state.deliveries) >= 1
                assert hook.state.deliveries[0]["headers"].get("authorization") == WEBHOOK_AUTH
            finally:
                cp.stop()
    finally:
        hook.stop()


def case_restart_recovery_and_uids():
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        cp = CandidateProcess(td_path, processing_steps=24, processing_step_seconds=0.05).start()
        try:
            t0 = enqueue_docs("persist", [{"id": 1, "title": "p"}])
            deadline = time.time() + 10
            seen_processing = False
            while time.time() < deadline:
                r = get_task(t0)
                assert r.status_code == 200, r.text
                task = r.json()
                if task[STATE_KEY] == RUNNING_STATE:
                    seen_processing = True
                    break
                time.sleep(0.05)
            assert seen_processing, "task never reached processing before restart"
        finally:
            cp.stop()
        cp2 = CandidateProcess(td_path, processing_steps=24, processing_step_seconds=0.05).start()
        try:
            finished = wait_task(t0, timeout=20)
            assert finished[STATE_KEY] == DONE_STATE
            assert [d["id"] for d in list_docs("persist")] == [1]
            t1 = enqueue_docs("persist", [{"id": 2, "title": "q"}])
            finished2 = wait_task(t1)
            assert finished2[STATE_KEY] == DONE_STATE
            assert t1 > t0
        finally:
            cp2.stop()

@pytest.fixture(scope="session")
def reference_stopped():
    stop_reference()


def test_integrity():
    check_probe_lab_integrity()


def test_batching_and_hook_payload(reference_stopped):
    case_batching_and_hook_payload()


def test_cancel_interrupts_batch(reference_stopped):
    case_cancel_interrupts_batch()


def test_task_deletion_filters(reference_stopped):
    case_task_deletion_filters()


def test_webhook_blocks_scheduler_and_logs_failures(reference_stopped):
    case_webhook_blocks_scheduler_and_logs_failures()


def test_restart_recovery_and_uids(reference_stopped):
    case_restart_recovery_and_uids()
