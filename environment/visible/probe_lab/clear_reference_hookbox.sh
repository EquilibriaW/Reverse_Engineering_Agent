#!/usr/bin/env bash
set -euo pipefail
curl -fsS -X POST http://127.0.0.1:19110/control/clear-hookbox >/dev/null
