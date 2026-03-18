#!/usr/bin/env bash
set -euo pipefail
for _ in $(seq 1 200); do
  if curl -fsS http://127.0.0.1:9108/health >/dev/null 2>&1; then
    exit 0
  fi
  sleep 0.1
done
echo "reference service did not become healthy" >&2
exit 1
