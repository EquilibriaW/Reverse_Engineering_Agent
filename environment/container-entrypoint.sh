#!/usr/bin/env bash
set -euo pipefail

mkdir -p /logs/verifier /workspace/scratch /workspace/probe_lab/reference_hookbox /workspace/candidate
chown -R agent:agent /workspace /logs/verifier

python /opt/internal/control/lab_control_plane.py >/var/log/probe_lab_control.log 2>&1 &
CONTROL_PID=$!

for _ in $(seq 1 200); do
  if curl -fsS http://127.0.0.1:19110/control/health >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

if ! curl -fsS http://127.0.0.1:19110/control/health >/dev/null 2>&1; then
  echo "control plane failed to start" >&2
  exit 1
fi

if [ "$#" -eq 0 ]; then
  su -s /bin/bash agent -c 'cd /workspace && sleep infinity'
  STATUS=$?
else
  printf -v CMD '%q ' "$@"
  su -s /bin/bash agent -c "cd /workspace && exec ${CMD}"
  STATUS=$?
fi

kill "$CONTROL_PID" >/dev/null 2>&1 || true
wait "$CONTROL_PID" >/dev/null 2>&1 || true
exit "$STATUS"
