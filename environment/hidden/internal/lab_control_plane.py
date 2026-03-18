import gzip
import importlib
import json
import os
import shutil
import sys
import threading
import time
from pathlib import Path
from typing import Callable, Optional

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
_ref_server: Optional[uvicorn.Server] = None
_ref_thread: Optional[threading.Thread] = None
_build_reference_app: Optional[Callable[[], FastAPI]] = None
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


def _configure_reference_env() -> None:
    os.environ.update({
        'ENGINE_DATA_DIR': str(STATE_DIR),
        'ENGINE_TASK_WEBHOOK_URL': f'http://127.0.0.1:{CONTROL_PORT}/hook',
        'ENGINE_TASK_WEBHOOK_AUTHORIZATION_HEADER': WEBHOOK_AUTH,
    })


def _load_reference_factory() -> Callable[[], FastAPI]:
    global _build_reference_app
    if _build_reference_app is not None:
        return _build_reference_app

    app_dir = str(REFERENCE_APP_FILE.parent)
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)
    module = importlib.import_module('runtime_contract_service')
    _build_reference_app = module.build_app
    return _build_reference_app


def _start_reference() -> None:
    global _ref_server, _ref_thread
    ensure_dirs()
    _configure_reference_env()
    build_app = _load_reference_factory()
    config = uvicorn.Config(build_app(), host=REFERENCE_HOST, port=REFERENCE_PORT, log_level='warning')
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    _ref_server = server
    _ref_thread = thread
    thread.start()
    try:
        _wait_healthy()
    except Exception:
        _stop_reference()
        raise


def _stop_reference() -> None:
    global _ref_server, _ref_thread
    if _ref_server is None:
        return
    server = _ref_server
    thread = _ref_thread
    _ref_server = None
    _ref_thread = None
    server.should_exit = True
    if thread is None:
        return
    thread.join(timeout=10)
    if thread.is_alive():
        server.force_exit = True
        thread.join(timeout=5)
    if thread.is_alive():
        raise RuntimeError('reference service failed to stop')


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
