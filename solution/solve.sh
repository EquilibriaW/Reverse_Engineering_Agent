#!/usr/bin/env bash
set -euo pipefail
ROOT="/workspace/candidate"
mkdir -p "$ROOT"
cat > "$ROOT/runtime_contract_service.py" <<'PYCODE'

import gzip
import json
import os
import threading
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from api_surface import (
    ABORT_JOBS_ROUTE,
    HEALTH_ROUTE,
    JOBS_ROUTE,
    JOB_ROUTE,
    STORE_RECORDS_ROUTE,
    STORE_RECORD_ROUTE,
    decode_filter_params,
    decode_records_payload,
    encode_document_list,
    encode_error_detail,
    encode_task,
    encode_task_list,
    encode_task_summary,
    encode_webhook_payload,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def duration_iso(seconds: float) -> str:
    return f'PT{seconds:.6f}S'


def atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    tmp.replace(path)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


TERMINAL_STATUSES = {'succeeded', 'failed', 'canceled'}


class QueueService:
    def __init__(self, data_dir: str, webhook_url: Optional[str] = None, webhook_authorization: Optional[str] = None, processing_steps: int = 16, processing_step_seconds: float = 0.05, scheduler_idle_seconds: float = 0.03):
        self.data_dir = Path(data_dir)
        self.state_path = self.data_dir / 'state.json'
        self.webhook_log_path = self.data_dir / 'scheduler.log'
        self.webhook_url = webhook_url
        self.webhook_authorization = webhook_authorization
        self.processing_steps = processing_steps
        self.processing_step_seconds = processing_step_seconds
        self.scheduler_idle_seconds = scheduler_idle_seconds
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.worker = None
        self.state = self._load_state()
        self._recover_processing_tasks()

    def _blank_state(self) -> Dict[str, Any]:
        return {
            'nextTaskUid': 0,
            'documents': {},
            'tasks': [],
        }

    def _load_state(self) -> Dict[str, Any]:
        ensure_dir(self.data_dir)
        if self.state_path.exists():
            with self.state_path.open('r', encoding='utf-8') as f:
                return json.load(f)
        return self._blank_state()

    def _save_state(self) -> None:
        atomic_write_json(self.state_path, self.state)

    def _recover_processing_tasks(self) -> None:
        changed = False
        with self.lock:
            for task in self.state['tasks']:
                if task['status'] == 'processing':
                    task['status'] = 'enqueued'
                    task['startedAt'] = None
                    task['finishedAt'] = None
                    task['duration'] = None
                    changed = True
            if changed:
                self._save_state()

    def start(self) -> None:
        self.worker = threading.Thread(target=self._run_worker, daemon=True)
        self.worker.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.worker:
            self.worker.join(timeout=5)

    def _run_worker(self) -> None:
        while not self.stop_event.is_set():
            processed = self._process_once()
            if not processed:
                time.sleep(self.scheduler_idle_seconds)

    def _iter_tasks_by_uid(self) -> List[Dict[str, Any]]:
        return sorted(self.state['tasks'], key=lambda t: t['uid'])

    def _task_summary(self, task: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'taskUid': task['uid'],
            'indexUid': task['indexUid'],
            'status': task['status'],
            'type': task['type'],
            'enqueuedAt': task['enqueuedAt'],
        }

    def _task_full(self, task: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'uid': task['uid'],
            'indexUid': task['indexUid'],
            'status': task['status'],
            'type': task['type'],
            'canceledBy': task['canceledBy'],
            'details': deepcopy(task['details']),
            'error': deepcopy(task['error']),
            'duration': task['duration'],
            'enqueuedAt': task['enqueuedAt'],
            'startedAt': task['startedAt'],
            'finishedAt': task['finishedAt'],
        }

    def _make_task(self, *, index_uid: Optional[str], task_type: str, payload: Optional[Dict[str, Any]] = None, details: Optional[Dict[str, Any]] = None, filters: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        uid = self.state['nextTaskUid']
        self.state['nextTaskUid'] += 1
        task = {
            'uid': uid,
            'indexUid': index_uid,
            'status': 'enqueued',
            'type': task_type,
            'canceledBy': None,
            'details': details or {},
            'payload': payload or {},
            'filter': filters or {},
            'error': None,
            'duration': None,
            'enqueuedAt': utc_now(),
            'startedAt': None,
            'finishedAt': None,
        }
        self.state['tasks'].append(task)
        self._save_state()
        return task

    def enqueue_document_add(self, index_uid: str, docs: List[Dict[str, Any]]) -> Dict[str, Any]:
        with self.lock:
            task = self._make_task(
                index_uid=index_uid,
                task_type='documentAdditionOrUpdate',
                payload={'documents': deepcopy(docs)},
                details={'receivedDocuments': len(docs), 'indexedDocuments': None},
            )
            return self._task_summary(task)

    def enqueue_cancel(self, filters: Dict[str, str]) -> Dict[str, Any]:
        if not filters:
            raise HTTPException(status_code=400, detail={
                'message': 'Task cancelation requires at least one filter.',
                'code': 'missing_task_filters',
                'type': 'invalid_request',
            })
        with self.lock:
            task = self._make_task(
                index_uid=None,
                task_type='taskCancelation',
                filters=filters,
                details={'matchedTasks': None, 'canceledTasks': None, 'originalFilter': deepcopy(filters)},
            )
            return self._task_summary(task)

    def enqueue_delete(self, filters: Dict[str, str]) -> Dict[str, Any]:
        if not filters:
            raise HTTPException(status_code=400, detail={
                'message': 'Task deletion requires at least one filter.',
                'code': 'missing_task_filters',
                'type': 'invalid_request',
            })
        with self.lock:
            task = self._make_task(
                index_uid=None,
                task_type='taskDeletion',
                filters=filters,
                details={'matchedTasks': None, 'deletedTasks': None, 'originalFilter': deepcopy(filters)},
            )
            return self._task_summary(task)

    def get_task(self, uid: int) -> Dict[str, Any]:
        with self.lock:
            for task in self.state['tasks']:
                if task['uid'] == uid:
                    return self._task_full(task)
        raise HTTPException(status_code=404, detail={
            'message': f'Task {uid} not found.',
            'code': 'task_not_found',
            'type': 'invalid_request',
        })

    def _parse_csv(self, value: Optional[str]) -> Optional[List[str]]:
        if value is None:
            return None
        return [piece for piece in value.split(',') if piece]

    def _match_task_filters(self, task: Dict[str, Any], filters: Dict[str, str]) -> bool:
        if 'uids' in filters:
            allowed = {int(x) for x in self._parse_csv(filters['uids']) or []}
            if task['uid'] not in allowed:
                return False
        if 'statuses' in filters:
            allowed = set(self._parse_csv(filters['statuses']) or [])
            if task['status'] not in allowed:
                return False
        if 'types' in filters:
            allowed = set(self._parse_csv(filters['types']) or [])
            if task['type'] not in allowed:
                return False
        if 'indexUids' in filters:
            allowed = set(self._parse_csv(filters['indexUids']) or [])
            if task['indexUid'] not in allowed:
                return False
        if 'canceledBy' in filters:
            try:
                cb = int(filters['canceledBy'])
            except ValueError:
                return False
            if task['canceledBy'] != cb:
                return False
        return True

    def list_tasks(self, filters: Dict[str, str]) -> Dict[str, Any]:
        with self.lock:
            tasks = [t for t in self.state['tasks'] if self._match_task_filters(t, filters)]
            tasks = sorted(tasks, key=lambda t: t['uid'], reverse=True)
            if 'from' in filters:
                try:
                    from_uid = int(filters['from'])
                    tasks = [t for t in tasks if t['uid'] < from_uid]
                except ValueError:
                    pass
            limit = int(filters.get('limit', '20'))
            results = [self._task_full(t) for t in tasks[:limit]]
            return {'results': results, 'limit': limit, 'total': len(tasks)}

    def get_document(self, index_uid: str, doc_id: str) -> Dict[str, Any]:
        with self.lock:
            index = self.state['documents'].get(index_uid, {})
            if doc_id not in index:
                raise HTTPException(status_code=404, detail={'message': 'Document not found', 'code': 'document_not_found', 'type': 'invalid_request'})
            return deepcopy(index[doc_id])

    def list_documents(self, index_uid: str) -> List[Dict[str, Any]]:
        with self.lock:
            index = self.state['documents'].get(index_uid, {})
            return [deepcopy(v) for _, v in sorted(index.items())]

    def _process_once(self) -> bool:
        with self.lock:
            enqueued = [t for t in self._iter_tasks_by_uid() if t['status'] == 'enqueued']
            if not enqueued:
                return False
            head = enqueued[0]
            if head['type'] == 'documentAdditionOrUpdate':
                if self._duration_seconds(head['enqueuedAt'], utc_now()) < self.scheduler_idle_seconds:
                    return False
            batch = [head]
            if head['type'] == 'documentAdditionOrUpdate':
                for task in enqueued[1:]:
                    if task['type'] == 'documentAdditionOrUpdate' and task['indexUid'] == head['indexUid']:
                        batch.append(task)
                    else:
                        break
            started_at = utc_now()
            for task in batch:
                task['status'] = 'processing'
                task['startedAt'] = started_at
                task['finishedAt'] = None
                task['duration'] = None
            self._save_state()
        if head['type'] == 'documentAdditionOrUpdate':
            self._process_document_batch(batch)
        elif head['type'] == 'taskCancelation':
            self._process_cancel_task(head)
        elif head['type'] == 'taskDeletion':
            self._process_delete_task(head)
        else:
            self._mark_batch_failed(batch, 'unknown_task_type', f'Unknown task type {head["type"]}')
        return True

    def _mark_batch_failed(self, batch: List[Dict[str, Any]], code: str, message: str) -> None:
        finished_at = utc_now()
        with self.lock:
            for task in batch:
                if task['status'] != 'processing':
                    continue
                started_at = task['startedAt']
                duration = duration_iso(max(0.0, self._duration_seconds(started_at, finished_at))) if started_at else None
                task['status'] = 'failed'
                task['error'] = {'code': code, 'message': message, 'type': 'runtime_error'}
                task['finishedAt'] = finished_at
                task['duration'] = duration
            self._save_state()
            payload = [self._task_full(t) for t in batch if t['status'] == 'failed']
        if payload:
            self._send_webhook(payload)

    def _duration_seconds(self, start: Optional[str], end: Optional[str]) -> float:
        if not start or not end:
            return 0.0
        start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
        return max(0.0, (end_dt - start_dt).total_seconds())

    def _process_document_batch(self, batch: List[Dict[str, Any]]) -> None:
        # Give cancellation tasks a realistic chance to interrupt the batch.
        for _ in range(self.processing_steps):
            if self.stop_event.is_set():
                return
            time.sleep(self.processing_step_seconds)
            cancel_task = self._pick_interrupting_cancel_task(batch)
            if cancel_task is not None:
                self._interrupt_with_cancel(batch, cancel_task)
                return
        finished_at = utc_now()
        with self.lock:
            batch_started = batch[0]['startedAt']
            index_uid = batch[0]['indexUid']
            index_store = self.state['documents'].setdefault(index_uid, {})
            succeeded = []
            failed = []
            for task in batch:
                if task['status'] != 'processing':
                    continue
                docs = task['payload'].get('documents', [])
                error = None
                for doc in docs:
                    if 'id' not in doc:
                        error = {'code': 'missing_primary_key', 'message': 'Document is missing the id field.', 'type': 'invalid_request'}
                        break
                duration = duration_iso(max(0.0, self._duration_seconds(batch_started, finished_at))) if batch_started else None
                task['finishedAt'] = finished_at
                task['duration'] = duration
                if error is None:
                    for doc in docs:
                        index_store[str(doc['id'])] = deepcopy(doc)
                    task['status'] = 'succeeded'
                    task['details']['indexedDocuments'] = len(docs)
                    task['error'] = None
                    succeeded.append(self._task_full(task))
                else:
                    task['status'] = 'failed'
                    task['details']['indexedDocuments'] = 0
                    task['error'] = error
                    failed.append(self._task_full(task))
            self._save_state()
        payload = succeeded + failed
        if payload:
            self._send_webhook(payload)

    def _pick_interrupting_cancel_task(self, current_batch: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        current_ids = {t['uid'] for t in current_batch}
        with self.lock:
            enqueued_cancellations = [t for t in self._iter_tasks_by_uid() if t['status'] == 'enqueued' and t['type'] == 'taskCancelation']
            for cancel_task in enqueued_cancellations:
                if self._matched_cancel_target_ids(cancel_task) & current_ids:
                    return cancel_task
        return None

    def _matched_cancel_target_ids(self, cancel_task: Dict[str, Any]) -> set[int]:
        return {
            task['uid']
            for task in self.state['tasks']
            if task['uid'] != cancel_task['uid']
            and task['status'] in {'enqueued', 'processing'}
            and self._match_task_filters(task, cancel_task['filter'])
        }

    def _finalize_cancel_task(self, cancel_task: Dict[str, Any], now: str, matched_count: int, canceled_count: int) -> None:
        cancel_task['status'] = 'succeeded'
        cancel_task['details'] = {
            'matchedTasks': matched_count,
            'canceledTasks': canceled_count,
            'originalFilter': deepcopy(cancel_task['filter']),
        }
        cancel_task['error'] = None
        cancel_task['finishedAt'] = now
        cancel_task['duration'] = duration_iso(max(0.0, self._duration_seconds(cancel_task['startedAt'], now)))

    def _apply_cancel_to_matching_tasks(self, cancel_task: Dict[str, Any], now: str) -> set[int]:
        matched_ids = self._matched_cancel_target_ids(cancel_task)
        canceled_count = 0
        for task in self.state['tasks']:
            if task['uid'] not in matched_ids:
                continue
            task['status'] = 'canceled'
            task['canceledBy'] = cancel_task['uid']
            task['finishedAt'] = now
            task['duration'] = duration_iso(max(0.0, self._duration_seconds(task.get('startedAt') or task['enqueuedAt'], now)))
            canceled_count += 1
        self._finalize_cancel_task(cancel_task, now, len(matched_ids), canceled_count)
        return matched_ids

    def _interrupt_with_cancel(self, current_batch: List[Dict[str, Any]], cancel_task: Dict[str, Any]) -> None:
        now = utc_now()
        current_ids = {t['uid'] for t in current_batch}
        with self.lock:
            cancel_task['status'] = 'processing'
            cancel_task['startedAt'] = now
            matched_ids = self._apply_cancel_to_matching_tasks(cancel_task, now)
            for task in self.state['tasks']:
                if task['uid'] in current_ids and task['uid'] not in matched_ids and task['status'] == 'processing':
                    # Progress is lost for unmatched tasks in the stopped batch.
                    task['status'] = 'enqueued'
                    task['startedAt'] = None
                    task['finishedAt'] = None
                    task['duration'] = None
            self._save_state()
            payload = [self._task_full(cancel_task)]
        self._send_webhook(payload)

    def _process_cancel_task(self, cancel_task: Dict[str, Any]) -> None:
        now = utc_now()
        with self.lock:
            self._apply_cancel_to_matching_tasks(cancel_task, now)
            self._save_state()
            payload = [self._task_full(cancel_task)]
        self._send_webhook(payload)

    def _process_delete_task(self, delete_task: Dict[str, Any]) -> None:
        now = utc_now()
        with self.lock:
            target_ids = [
                t['uid'] for t in self.state['tasks']
                if t['uid'] != delete_task['uid'] and t['status'] in TERMINAL_STATUSES and self._match_task_filters(t, delete_task['filter'])
            ]
            self.state['tasks'] = [t for t in self.state['tasks'] if t['uid'] not in set(target_ids)]
            delete_task['status'] = 'succeeded'
            delete_task['details'] = {
                'matchedTasks': len(target_ids),
                'deletedTasks': len(target_ids),
                'originalFilter': deepcopy(delete_task['filter']),
            }
            delete_task['error'] = None
            delete_task['finishedAt'] = now
            delete_task['duration'] = duration_iso(max(0.0, self._duration_seconds(delete_task['startedAt'], now)))
            self.state['tasks'].append(delete_task)
            self.state['tasks'] = sorted(self.state['tasks'], key=lambda t: t['uid'])
            self._save_state()
            payload = [self._task_full(delete_task)]
        self._send_webhook(payload)

    def _send_webhook(self, payload: List[Dict[str, Any]]) -> None:
        if not self.webhook_url:
            return
        lines = '\n'.join(json.dumps(item, separators=(',', ':')) for item in encode_webhook_payload(payload)).encode('utf-8')
        compressed = gzip.compress(lines)
        headers = {
            'Content-Type': 'application/jsonl',
            'Content-Encoding': 'gzip',
        }
        if self.webhook_authorization:
            headers['Authorization'] = self.webhook_authorization
        try:
            response = requests.post(self.webhook_url, data=compressed, headers=headers, timeout=10)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - used in integration behavior
            self.webhook_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.webhook_log_path.open('a', encoding='utf-8') as f:
                f.write(f'{utc_now()} webhook_error {exc}\n')

def build_app() -> FastAPI:
    data_dir = os.environ.get('ENGINE_DATA_DIR', '/tmp/engine-data')
    webhook_url = os.environ.get('ENGINE_TASK_WEBHOOK_URL')
    webhook_authorization = os.environ.get('ENGINE_TASK_WEBHOOK_AUTHORIZATION_HEADER')
    processing_steps = int(os.environ.get('ENGINE_PROCESSING_STEPS', '16'))
    processing_step_seconds = float(os.environ.get('ENGINE_PROCESSING_STEP_SECONDS', '0.05'))
    service = QueueService(
        data_dir=data_dir,
        webhook_url=webhook_url,
        webhook_authorization=webhook_authorization,
        processing_steps=processing_steps,
        processing_step_seconds=processing_step_seconds,
    )

    app = FastAPI(title='Probe Queue Contract Service')

    @app.on_event('startup')
    async def _startup() -> None:
        service.start()

    @app.on_event('shutdown')
    async def _shutdown() -> None:
        service.stop()

    @app.exception_handler(HTTPException)
    async def _http_exception_handler(_: Request, exc: HTTPException):
        detail = exc.detail
        if isinstance(detail, dict):
            return JSONResponse(status_code=exc.status_code, content=encode_error_detail(detail))
        return JSONResponse(status_code=exc.status_code, content={'message': detail})

    @app.get(HEALTH_ROUTE)
    async def health() -> Dict[str, str]:
        return {'status': 'available'}

    @app.post(STORE_RECORDS_ROUTE)
    async def add_documents(index_uid: str, request: Request):
        body = await request.json()
        docs = decode_records_payload(body)
        if docs is None:
            raise HTTPException(status_code=400, detail={'message': 'Body must be a JSON object with a records array.', 'code': 'invalid_documents_payload', 'type': 'invalid_request'})
        summary = service.enqueue_document_add(index_uid, docs)
        return JSONResponse(status_code=202, content=encode_task_summary(summary))

    @app.get(STORE_RECORD_ROUTE)
    async def get_document(index_uid: str, doc_id: str):
        return service.get_document(index_uid, doc_id)

    @app.get(STORE_RECORDS_ROUTE)
    async def list_documents(index_uid: str):
        return encode_document_list(service.list_documents(index_uid))

    @app.get(JOB_ROUTE)
    async def get_task(task_uid: int):
        return encode_task(service.get_task(task_uid))

    @app.get(JOBS_ROUTE)
    async def list_tasks(request: Request):
        filters = decode_filter_params(request.query_params)
        return encode_task_list(service.list_tasks(filters))

    @app.post(ABORT_JOBS_ROUTE)
    async def cancel_tasks(request: Request):
        filters = decode_filter_params(request.query_params)
        filters.pop('limit', None)
        filters.pop('from', None)
        summary = service.enqueue_cancel(filters)
        return JSONResponse(status_code=202, content=encode_task_summary(summary))

    @app.delete(JOBS_ROUTE)
    async def delete_tasks(request: Request):
        filters = decode_filter_params(request.query_params)
        filters.pop('limit', None)
        filters.pop('from', None)
        summary = service.enqueue_delete(filters)
        return JSONResponse(status_code=202, content=encode_task_summary(summary))

    return app


app = build_app()
PYCODE
cp "$(cd "$(dirname "$0")" && pwd)/api_surface.py" "$ROOT/api_surface.py"
cat > "$ROOT/run.sh" <<'BASH'
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
: "${ENGINE_DATA_DIR:=$ROOT/state}"
export ENGINE_DATA_DIR
exec python -m uvicorn runtime_contract_service:app --host 127.0.0.1 --port 8108 --app-dir "$ROOT" --log-level warning
BASH
chmod +x "$ROOT/run.sh"
