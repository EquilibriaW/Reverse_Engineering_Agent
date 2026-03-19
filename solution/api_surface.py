from copy import deepcopy
from typing import Any, Dict, List, Mapping, Optional

HEALTH_ROUTE = "/health"
STORE_RECORDS_ROUTE = "/stores/{index_uid}/records"
STORE_RECORD_ROUTE = "/stores/{index_uid}/records/{doc_id}"
JOBS_ROUTE = "/jobs"
JOB_ROUTE = "/jobs/{task_uid}"
ABORT_JOBS_ROUTE = "/jobs/abort"

RECORDS_BODY_KEY = "records"
ITEMS_KEY = "items"
PAGE_SIZE_KEY = "pageSize"
TOTAL_KEY = "total"
JOB_ID_KEY = "jobId"
STORE_ID_KEY = "storeId"
STATE_KEY = "state"
KIND_KEY = "kind"
ABORTED_BY_KEY = "abortedBy"
META_KEY = "meta"
FAULT_KEY = "fault"
ELAPSED_KEY = "elapsed"
QUEUED_AT_KEY = "queuedAt"
STARTED_AT_KEY = "startedAt"
ENDED_AT_KEY = "endedAt"
RECEIVED_RECORDS_KEY = "receivedRecords"
STORED_RECORDS_KEY = "storedRecords"
MATCHED_JOBS_KEY = "matchedJobs"
ABORTED_JOBS_KEY = "abortedJobs"
REMOVED_JOBS_KEY = "removedJobs"
ORIGINAL_QUERY_KEY = "originalQuery"
INVALID_RECORDS_PAYLOAD_CODE = "invalid_records_payload"
MISSING_JOB_FILTERS_CODE = "missing_job_filters"
JOB_NOT_FOUND_CODE = "job_not_found"
RECORD_NOT_FOUND_CODE = "record_not_found"
MISSING_RECORD_KEY_CODE = "missing_record_key"
UNKNOWN_JOB_KIND_CODE = "unknown_job_kind"
QUEUED_STATE = "queued"
RUNNING_STATE = "running"
DONE_STATE = "done"
ERRORED_STATE = "errored"
ABORTED_STATE = "aborted"
RECORD_WRITE_KIND = "recordWrite"
JOB_ABORT_KIND = "jobAbort"
JOB_PRUNE_KIND = "jobPrune"

STATUS_TO_EXTERNAL = {
    "enqueued": "queued",
    "processing": "running",
    "succeeded": "done",
    "failed": "errored",
    "canceled": "aborted",
}
STATUS_TO_INTERNAL = {value: key for key, value in STATUS_TO_EXTERNAL.items()}

TASK_TYPE_TO_EXTERNAL = {
    "documentAdditionOrUpdate": "recordWrite",
    "taskCancelation": "jobAbort",
    "taskDeletion": "jobPrune",
}
TASK_TYPE_TO_INTERNAL = {value: key for key, value in TASK_TYPE_TO_EXTERNAL.items()}

FILTER_KEY_TO_EXTERNAL = {
    "uids": "ids",
    "statuses": "states",
    "types": "kinds",
    "indexUids": "storeIds",
    "canceledBy": "abortedBy",
    "limit": PAGE_SIZE_KEY,
    "from": "beforeId",
}
FILTER_KEY_TO_INTERNAL = {value: key for key, value in FILTER_KEY_TO_EXTERNAL.items()}

DETAIL_KEY_TO_EXTERNAL = {
    "receivedDocuments": RECEIVED_RECORDS_KEY,
    "indexedDocuments": STORED_RECORDS_KEY,
    "matchedTasks": MATCHED_JOBS_KEY,
    "canceledTasks": ABORTED_JOBS_KEY,
    "deletedTasks": REMOVED_JOBS_KEY,
    "originalFilter": ORIGINAL_QUERY_KEY,
}

ERROR_CODE_TO_EXTERNAL = {
    "invalid_documents_payload": INVALID_RECORDS_PAYLOAD_CODE,
    "missing_task_filters": MISSING_JOB_FILTERS_CODE,
    "task_not_found": JOB_NOT_FOUND_CODE,
    "document_not_found": RECORD_NOT_FOUND_CODE,
    "missing_primary_key": MISSING_RECORD_KEY_CODE,
    "unknown_task_type": UNKNOWN_JOB_KIND_CODE,
}

ERROR_TYPE_TO_EXTERNAL = {
    "invalid_request": "invalid_input",
    "runtime_error": "runtime_issue",
}


def stores_records_path(index_uid: str) -> str:
    return STORE_RECORDS_ROUTE.format(index_uid=index_uid)


def store_record_path(index_uid: str, doc_id: str) -> str:
    return STORE_RECORD_ROUTE.format(index_uid=index_uid, doc_id=doc_id)


def jobs_path() -> str:
    return JOBS_ROUTE


def job_path(task_uid: int) -> str:
    return JOB_ROUTE.format(task_uid=task_uid)


def abort_jobs_path() -> str:
    return ABORT_JOBS_ROUTE


def decode_records_payload(body: Any) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(body, dict):
        return None
    records = body.get(RECORDS_BODY_KEY)
    if not isinstance(records, list):
        return None
    return records


def encode_document_list(documents: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {ITEMS_KEY: deepcopy(documents)}


def decode_filter_params(query_params: Mapping[str, str]) -> Dict[str, str]:
    filters: Dict[str, str] = {}
    for key, value in query_params.items():
        if value is None or value == "":
            continue
        internal_key = FILTER_KEY_TO_INTERNAL.get(key)
        if not internal_key:
            continue
        filters[internal_key] = _decode_filter_value(internal_key, value)
    return filters


def encode_task_summary(task: Dict[str, Any]) -> Dict[str, Any]:
    return {
        JOB_ID_KEY: task["taskUid"],
        STORE_ID_KEY: task["indexUid"],
        STATE_KEY: _encode_status(task["status"]),
        KIND_KEY: _encode_task_type(task["type"]),
        QUEUED_AT_KEY: task["enqueuedAt"],
    }


def encode_task(task: Dict[str, Any]) -> Dict[str, Any]:
    return {
        JOB_ID_KEY: task["uid"],
        STORE_ID_KEY: task["indexUid"],
        STATE_KEY: _encode_status(task["status"]),
        KIND_KEY: _encode_task_type(task["type"]),
        ABORTED_BY_KEY: task["canceledBy"],
        META_KEY: _encode_details(task.get("details")),
        FAULT_KEY: _encode_error(task.get("error")),
        ELAPSED_KEY: task["duration"],
        QUEUED_AT_KEY: task["enqueuedAt"],
        STARTED_AT_KEY: task["startedAt"],
        ENDED_AT_KEY: task["finishedAt"],
    }


def encode_task_list(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        ITEMS_KEY: [encode_task(task) for task in payload["results"]],
        PAGE_SIZE_KEY: payload["limit"],
        TOTAL_KEY: payload["total"],
    }


def encode_webhook_payload(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [encode_task(task) for task in tasks]


def encode_error_detail(detail: Any) -> Any:
    if not isinstance(detail, dict):
        return detail
    encoded = deepcopy(detail)
    code = encoded.get("code")
    if code in ERROR_CODE_TO_EXTERNAL:
        encoded["code"] = ERROR_CODE_TO_EXTERNAL[code]
    error_type = encoded.get("type")
    if error_type in ERROR_TYPE_TO_EXTERNAL:
        encoded["type"] = ERROR_TYPE_TO_EXTERNAL[error_type]
    return encoded


def _encode_status(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return STATUS_TO_EXTERNAL.get(value, value)


def _encode_task_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return TASK_TYPE_TO_EXTERNAL.get(value, value)


def _decode_filter_value(key: str, value: str) -> str:
    if key in {"statuses", "types"}:
        values = [piece for piece in value.split(",") if piece]
        if key == "statuses":
            decoded = [STATUS_TO_INTERNAL.get(piece, piece) for piece in values]
        else:
            decoded = [TASK_TYPE_TO_INTERNAL.get(piece, piece) for piece in values]
        return ",".join(decoded)
    return value


def _encode_filter_value(key: str, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    if key in {"statuses", "types"}:
        values = [piece for piece in value.split(",") if piece]
        if key == "statuses":
            encoded = [_encode_status(piece) for piece in values]
        else:
            encoded = [_encode_task_type(piece) for piece in values]
        return ",".join(piece for piece in encoded if piece)
    return value


def _encode_filter_dict(filters: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if filters is None:
        return None
    encoded: Dict[str, Any] = {}
    for key, value in filters.items():
        external_key = FILTER_KEY_TO_EXTERNAL.get(key, key)
        encoded[external_key] = _encode_filter_value(key, value)
    return encoded


def _encode_details(details: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if details is None:
        return None
    encoded: Dict[str, Any] = {}
    for key, value in details.items():
        external_key = DETAIL_KEY_TO_EXTERNAL.get(key, key)
        if key == "originalFilter":
            encoded[external_key] = _encode_filter_dict(value)
        else:
            encoded[external_key] = deepcopy(value)
    return encoded


def _encode_error(error: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if error is None:
        return None
    return encode_error_detail(error)
