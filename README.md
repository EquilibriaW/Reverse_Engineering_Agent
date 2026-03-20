# Runtime Probe Queue Task

This repository contains a Harbor reverse-engineering task built around a small async queue / document-write service.

The agent is given:
- a visible probe lab,
- a hidden black-box reference service on `127.0.0.1:9108`,
- a workspace where it must build a compatible service on `127.0.0.1:8108`,
- and a verifier that scores only externally visible behavior.

The task is intentionally about runtime behavior, not source recovery: queue ordering, batching, cancellation, deletion, webhook delivery, and persistence across restart.

## Project Structure

### Top-level files

- `task.toml`
  Harbor task metadata and resource limits.

- `instruction.md`
  The agent-facing task prompt. This defines the visible API surface, the probing workflow, and the runtime constraints.

- `README.md`
  This file.

### `environment/`

- `environment/Dockerfile`
  Builds the task environment image. It packages:
  - the hidden reference service,
  - the hidden control plane,
  - the visible probe-lab helpers,
  - and the candidate workspace scaffold.

- `environment/container-entrypoint.sh`
  Container boot logic. It:
  - starts the control plane,
  - waits for it to become healthy,
  - deletes `/opt/internal`,
  - then drops to the `agent` user and runs the harness command.

  The deletion step matters because Harbor agents may effectively run with root-level visibility in the container. The hidden source therefore has to be removed, not merely permission-protected.

### `environment/hidden/internal/`

- `environment/hidden/internal/runtime_contract_service.py`
  The hidden reference implementation. This is the ground-truth behavior the agent is supposed to infer by probing.

- `environment/hidden/internal/lab_control_plane.py`
  A small local control plane used by the probe lab and verifier. It:
  - starts the reference service in-process,
  - supports reset / restart / stop,
  - captures webhook deliveries into `reference_hookbox/`.

- `environment/hidden/internal/api_surface.py`
  The canonical public API mapping used for the obfuscated surface: route names, filter names, response keys, state names, kind names, and error-code remapping.

### `environment/visible/`

- `environment/visible/probe_lab/README.md`
  Explains the visible reference endpoint, captured webhook files, and helper scripts.

- `environment/visible/probe_lab/reset_reference.sh`
  Wipes reference state and restarts it.

- `environment/visible/probe_lab/restart_reference.sh`
  Restarts the reference service without wiping state.

- `environment/visible/probe_lab/clear_reference_hookbox.sh`
  Clears captured reference webhooks.

- `environment/visible/probe_lab/wait_reference.sh`
  Waits for the hidden reference to become healthy.

- `environment/visible/candidate.README.txt`
  Minimal scaffold note inside `/workspace/candidate`.

### `solution/`

- `solution/solve.sh`
  The oracle implementation. It writes a complete candidate service into `/workspace/candidate`, including `run.sh`.

- `solution/api_surface.py`
  A mirrored copy of the canonical API mapping for the oracle path.

  This mirror is intentional: Harbor uploads `solution/` to the oracle, so the oracle needs its own copy even though the canonical source lives under `environment/hidden/internal/`.
  Update mirrors with `python scripts/sync_api_surface.py` and verify them with `python scripts/sync_api_surface.py --check`.

### `tests/`

- `tests/test.sh`
  Verifier entrypoint. It runs `pytest` over `test_state.py`, then writes `reward.txt` and `report.json`.

- `tests/test_state.py`
  The real verifier. It:
  - checks probe-lab integrity,
  - starts and stops the candidate repeatedly,
  - drives end-to-end HTTP behavior checks,
  - exposes each weighted behavior check as a pytest test.

- `tests/api_surface.py`
  A mirrored copy of the canonical API mapping for the verifier path.

  This mirror is also intentional: Harbor uploads `tests/` to the verifier separately from the environment image.
  Update mirrors with `python scripts/sync_api_surface.py` and verify them with `python scripts/sync_api_surface.py --check`.

## Public Surface

The task exposes this obfuscated API:

- `GET /health`
- `POST /stores/{index_uid}/records`
- `GET /stores/{index_uid}/records`
- `GET /stores/{index_uid}/records/{doc_id}`
- `GET /jobs`
- `GET /jobs/{task_uid}`
- `POST /jobs/abort`
- `DELETE /jobs`

The obfuscation layer also renames:
- task fields,
- filter names,
- state names,
- task-kind names,
- and several error codes.

That mapping is centralized in `api_surface.py`.

## What The Tests Check

The verifier is integration-style. It does not inspect candidate internals.

Weighted cases in `tests/test_state.py`:

- `integrity` (`0.15`)
  Confirms the probe-lab assets were not modified and the hidden reference is still healthy.

- `batching_and_hook_payload` (`0.20`)
  Checks same-store write batching, cross-store separation, and webhook payload formatting.

- `cancel_interrupts_batch` (`0.25`)
  Checks that an abort job can interrupt an in-flight write batch and mark later writes as aborted.

- `task_deletion_filters` (`0.15`)
  Checks filtered deletion of terminal jobs and not-found behavior afterward.

- `webhook_blocks_scheduler_and_logs_failures` (`0.10`)
  Checks that webhook delivery can block later work and that webhook failures append `webhook_error` to `scheduler.log`.

- `restart_recovery_and_uids` (`0.15`)
  Checks that in-flight work recovers after restart, persisted state survives, and job IDs continue increasing.

### Critical behaviors the agent must infer

These are the easiest places for a candidate to look plausible while still being wrong:

- Write batching is based on contiguous same-store `recordWrite` jobs. Batched jobs should share start / end timestamps and elapsed time. Writes for different stores should not be coalesced together.

- `POST /jobs/abort` is itself asynchronous. The resulting `jobAbort` can interrupt an already-running write batch, abort matching queued or running jobs, and force unmatched work from the interrupted batch back into the queue.

- `DELETE /jobs` is also asynchronous and filter-driven. It requires at least one filter and only prunes already-terminal jobs. The prune job itself stays visible after completion.

- Webhooks are part of the scheduler path, not a background side effect. Successful deliveries are gzip-compressed JSON Lines with the configured auth header, and a blocked webhook can stall later work. Failed deliveries must append `webhook_error` to `scheduler.log`.

- Restart behavior is not "resume the exact thread". Jobs that were `running` at shutdown are expected to come back as queued work after restart, persisted documents must survive, and job IDs must remain monotonic across restarts.

### What is task success?

I've tested about 10 trials of Opus 4.6. It achieves an average reward of roughly `0.45`. The right way to view this task is pass@k: success means passing the full verifier, not just getting partial credit. Across those ten trials, I saw one full-score pass.

### Async status handling

The verifier accepts either `200` or `202` for the asynchronous task-creation endpoints:

- `POST /stores/{index_uid}/records`
- `POST /jobs/abort`
- `DELETE /jobs`

- `202` is the intended async-contract status.
- `200` is tolerated so otherwise-interesting runs are not discarded immediately.
- If any of those endpoints returns `200` instead of `202`, the verifier applies a one-time `0.05` penalty.

## How This Task Was Mined And Adapted

This task was mined from a narrow Meilisearch-like slice of behavior:

- async document ingestion,
- task lifecycle objects,
- queue filtering,
- cancellation and deletion,
- webhook emission,
- restart persistence.

It was then adapted into a Harbor-compatible reverse-engineering task:

- The scope was narrowed to a bounded HTTP slice rather than a full product.
- The hidden reference was embedded directly in the environment so agents can probe behavior locally without internet access.
- A visible probe lab was added so agents can reset the reference, restart it, and inspect webhook captures.
- An oracle solution was added so the task can be regression-tested end to end.

## Obfuscation

The original public-looking names were intentionally removed.

Instead of exposing obvious Meilisearch-flavored names directly, the task now:

- remaps routes, query parameters, response fields, states, kinds, and error codes through `api_surface.py`,
- keeps that mapping in a single canonical file for auditability,
- mirrors it only into `solution/` and `tests/` because Harbor uploads those directories separately,
- and removes `/opt/internal` at container startup so the mapping and hidden reference code are not readable by the agent at runtime.

The goal is not to hide the behavioral contract from probing. The goal is to reduce easy training-data recall and force evaluation to depend more on behavior discovery than on memorized product-specific names.
