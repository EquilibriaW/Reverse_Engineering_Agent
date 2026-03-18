
import gzip
import json
import os
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

import requests
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

REFERENCE_HOST = '127.0.0.1'
REFERENCE_PORT = 9108
CONTROL_PORT = 19110

BASE_DIR = Path(os.environ.get('LAB_BASE_DIR', '/workspace/probe_lab')).resolve()
HOOKBOX_DIR = BASE_DIR / 'reference_hookbox'
STATE_DIR = Path(os.environ.get('REFERENCE_STATE_DIR', '/var/lib/reference_runtime')).resolve()
REFERENCE_APP_FILE = Path(os.environ.get('REFERENCE_APP_FILE', '/opt/internal/reference_service/runtime_contract_service.py')).resolve()
WEBHOOK_AUTH = 'Bearer probe-lab-secret'

app = FastAPI(title='Probe Lab Control Plane')
_ref_proc: Optional[subprocess.Popen] = None
_batch_counter = 0
_lock = threading.RLock()


def _wait_healthy(timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f'http://{REFERENCE_HOST}:{REFERENCE_PORT}/health', timeout=0.5)
            if r.ok:
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError('reference service failed to become healthy')


def _start_reference() -> None:
    global _ref_proc
    ensure_dirs()
    env = os.environ.copy()
    env.update({
        'ENGINE_DATA_DIR': str(STATE_DIR),
        'ENGINE_TASK_WEBHOOK_URL': f'http://127.0.0.1:{CONTROL_PORT}/hook',
        'ENGINE_TASK_WEBHOOK_AUTHORIZATION_HEADER': WEBHOOK_AUTH,
    })
    _ref_proc = subprocess.Popen([
        'python', '-m', 'uvicorn', 'runtime_contract_service:app',
        '--host', REFERENCE_HOST,
        '--port', str(REFERENCE_PORT),
        '--app-dir', str(REFERENCE_APP_FILE.parent),
        '--log-level', 'warning',
    ], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _wait_healthy()


def _stop_reference() -> None:
    global _ref_proc
    if _ref_proc is None:
        return
    proc = _ref_proc
    _ref_proc = None
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def ensure_dirs() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    HOOKBOX_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def reset_hookbox() -> None:
    if HOOKBOX_DIR.exists():
        shutil.rmtree(HOOKBOX_DIR)
    HOOKBOX_DIR.mkdir(parents=True, exist_ok=True)


@app.on_event('startup')
def startup() -> None:
    ensure_dirs()
    reset_hookbox()
    _start_reference()


@app.on_event('shutdown')
def shutdown() -> None:
    _stop_reference()


@app.get('/control/health')
def health():
    return {'status': 'ok'}


@app.post('/control/reset-reference')
def reset_reference():
    with _lock:
        _stop_reference()
        if STATE_DIR.exists():
            shutil.rmtree(STATE_DIR)
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        reset_hookbox()
        _start_reference()
    return {'status': 'reset'}


@app.post('/control/restart-reference')
def restart_reference():
    with _lock:
        _stop_reference()
        _start_reference()
    return {'status': 'restarted'}




@app.post('/control/stop-reference')
def stop_reference():
    with _lock:
        _stop_reference()
    return {'status': 'stopped'}

@app.post('/control/clear-hookbox')
def clear_hookbox():
    with _lock:
        reset_hookbox()
    return {'status': 'cleared'}


@app.post('/hook')
async def hook(request: Request):
    global _batch_counter
    ensure_dirs()
    body = await request.body()
    headers = {k.lower(): v for k, v in request.headers.items()}
    _batch_counter += 1
    stem = f'batch_{_batch_counter:04d}'
    (HOOKBOX_DIR / f'{stem}.headers.json').write_text(json.dumps(headers, indent=2, sort_keys=True), encoding='utf-8')
    (HOOKBOX_DIR / f'{stem}.raw.gz').write_bytes(body)
    try:
        decoded = gzip.decompress(body).decode('utf-8')
    except Exception:
        decoded = body.decode('utf-8', errors='replace')
    (HOOKBOX_DIR / f'{stem}.payload.jsonl').write_text(decoded, encoding='utf-8')
    return JSONResponse(status_code=202, content={'ok': True})


if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=CONTROL_PORT, log_level='warning')
