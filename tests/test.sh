#!/usr/bin/env bash
set -euo pipefail
mkdir -p /logs/verifier
TEST_FILE="$(dirname "$0")/test_state.py"
JUNIT_PATH="/logs/verifier/junit.xml"
STATUS_LOG_PATH="/logs/verifier/async_statuses.log"

rm -f "$JUNIT_PATH" "$STATUS_LOG_PATH" /logs/verifier/reward.txt /logs/verifier/report.json

set +e
python3 -m pytest --junitxml "$JUNIT_PATH" -rA "$TEST_FILE"
set -e

python3 - <<'PY'
import json
import xml.etree.ElementTree as ET
from pathlib import Path

reward_path = Path("/logs/verifier/reward.txt")
report_path = Path("/logs/verifier/report.json")
junit_path = Path("/logs/verifier/junit.xml")
status_log_path = Path("/logs/verifier/async_statuses.log")

cases = [
    ("test_integrity", "integrity", 0.15),
    ("test_batching_and_hook_payload", "batching_and_hook_payload", 0.20),
    ("test_cancel_interrupts_batch", "cancel_interrupts_batch", 0.25),
    ("test_task_deletion_filters", "task_deletion_filters", 0.15),
    ("test_webhook_blocks_scheduler_and_logs_failures", "webhook_blocks_scheduler_and_logs_failures", 0.10),
    ("test_restart_recovery_and_uids", "restart_recovery_and_uids", 0.15),
]
expected_async_status = 202
async_status_penalty = 0.05

outcomes = {}
if junit_path.exists():
    root = ET.parse(junit_path).getroot()
    for testcase in root.iter("testcase"):
        outcomes[testcase.attrib["name"]] = testcase

results = []
for test_name, case_name, weight in cases:
    testcase = outcomes.get(test_name)
    if testcase is None:
        results.append(
            {
                "name": case_name,
                "passed": False,
                "weight": weight,
                "error": "test did not run",
            }
        )
        continue

    failure = testcase.find("failure")
    error = testcase.find("error")
    passed = failure is None and error is None
    result = {"name": case_name, "passed": passed, "weight": weight}
    if not passed:
        detail = failure if failure is not None else error
        message = detail.attrib.get("message") or (detail.text or "").strip()
        result["error"] = message or "pytest failure"
    results.append(result)

reward = sum(result["weight"] for result in results if result["passed"])
penalties = []

if status_log_path.exists():
    async_statuses = [
        int(line.strip())
        for line in status_log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
else:
    async_statuses = []

non_async_status_count = sum(
    status != expected_async_status for status in async_statuses
)
if non_async_status_count:
    reward = max(0.0, reward - async_status_penalty)
    penalties.append(
        {
            "name": "async_status_not_202",
            "amount": async_status_penalty,
            "count": non_async_status_count,
            "expected": expected_async_status,
            "observed": sorted(set(async_statuses)),
        }
    )

report = {"reward": reward, "results": results}
if penalties:
    report["penalties"] = penalties

reward_path.write_text(f"{reward:.4f}\n", encoding="utf-8")
report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
PY
