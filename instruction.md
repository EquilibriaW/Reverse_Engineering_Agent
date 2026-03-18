# Task

Build an HTTP service under `/workspace/candidate` that is compatible with a hidden verifier.

The verifier only cares about **external service behavior** for a bounded slice.
It does **not** care about matching any internal module layout, class hierarchy, filenames, or implementation language choices inside `/workspace/candidate`.

## In-scope resource inventory

Your implementation must expose these HTTP resources on `127.0.0.1:8108`:

- `GET /health`
- `POST /indexes/{index_uid}/documents`
- `GET /indexes/{index_uid}/documents`
- `GET /indexes/{index_uid}/documents/{doc_id}`
- `GET /tasks`
- `GET /tasks/{task_uid}`
- `POST /tasks/cancel`
- `DELETE /tasks`

## In-scope behavioral inventory

The hidden verifier stresses runtime behavior for this slice:
- asynchronous task lifecycle for mutating operations
- queue ordering over time
- batch behavior for some writes
- cancellation and deletion driven by task filters
- webhook delivery behavior for completed work
- persistence across restart

A black-box reference service is already running on `127.0.0.1:9108`.
Use it to probe the product contract.

Helpful assets:
- `/workspace/probe_lab/README.md`
- `/workspace/probe_lab/reset_reference.sh`
- `/workspace/probe_lab/restart_reference.sh`
- `/workspace/probe_lab/clear_reference_hookbox.sh`
- `/workspace/probe_lab/wait_reference.sh`
- `/workspace/probe_lab/reference_hookbox/`
- `/workspace/scratch/`

## Practical guidance

This is **not** a schema-recovery puzzle.
The main challenge is understanding how the service behaves over time.
A productive approach is usually:
1. probe the reference service,
2. observe how task objects evolve,
3. watch what shows up in `reference_hookbox/`,
4. restart the reference service to see which state survives,
5. then implement only the externally visible slice.

## Constraints

1. Put your implementation in `/workspace/candidate`.
2. You must create `/workspace/candidate/run.sh`.
3. `run.sh` must start a server on `127.0.0.1:8108` and block.
4. Do not modify `/workspace/probe_lab/README.md` or the helper scripts in `/workspace/probe_lab/`.
5. Do not stop, replace, or proxy to the reference service. Hidden scoring will disable the reference service before compatibility checks.
6. Hidden scoring will start and stop your implementation multiple times.
7. External network access is not allowed.

## Runtime contract hints

Your service should honor these environment variables when present:
- `ENGINE_DATA_DIR`
- `ENGINE_TASK_WEBHOOK_URL`
- `ENGINE_TASK_WEBHOOK_AUTHORIZATION_HEADER`
- `ENGINE_PROCESSING_STEPS`
- `ENGINE_PROCESSING_STEP_SECONDS`

When webhook delivery fails (non-2xx response or network error), your service
must append a line containing `webhook_error` to `scheduler.log` inside the
data directory.

The reference service uses the same environment variable names.
