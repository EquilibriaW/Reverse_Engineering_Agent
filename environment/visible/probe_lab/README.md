# Probe Lab

Reference service:
- Base URL: `http://127.0.0.1:9108`
- Health route: `GET /health`

Reference webhook sink:
- Captured deliveries are written into `reference_hookbox/`
- `*.raw.gz` is the raw request body captured from the reference service
- `*.payload.jsonl` is the decoded payload when gzip / JSON Lines decoding succeeds
- `*.headers.json` contains request headers seen by the sink

Control helpers:
- `reset_reference.sh` wipes the reference state and restarts it
- `restart_reference.sh` restarts the reference service without wiping state
- `clear_reference_hookbox.sh` clears captured webhook deliveries
- `wait_reference.sh` waits until the reference service is healthy

Do not edit this README or the helper scripts.
You may read them. You may also inspect files inside `reference_hookbox/` and `scratch/`.
