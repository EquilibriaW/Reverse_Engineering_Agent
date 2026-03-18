#!/usr/bin/env bash
set -euo pipefail
mkdir -p /logs/verifier
python3 "$(dirname "$0")/test_state.py"
