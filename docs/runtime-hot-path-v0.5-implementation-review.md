# ForgeReplay v0.5 Runtime Hot-Path Implementation Review

Date: 2026-08-21  
Branch: `codex/runtime-hot-path-v0.5`  
Source plan: `docs/plans/2026-08-21-runtime-hot-path-projection-optimization.md`

## Outcome

The Runtime no longer reconstructs full run history to find an unfinished tool,
an unconsumed model response, the next model step, or the retry attempt offset.
These decisions now use bounded indexed queries against the existing
`tool_calls` projection and the new transactional `model_calls` projection.

Checkpoint creation, phase transitions, successful completion, termination,
and normal projection reads now recover from the newest valid checkpoint plus
its event tail. Checkpoint rows are inspected lazily from newest to oldest, so
the common valid-checkpoint path does not materialize all historical snapshots.
Full event replay remains an intentional integrity fallback when every
checkpoint is invalid.

## Pre-implementation review corrections

The implementation deliberately tightened several parts of the proposed plan:

- `latest_attempt_no`, rather than `attempt_count`, is the retry offset. This
  prevents a legacy sequence such as attempts 1 and 3 from reusing attempt 3.
- A partial unique index allows at most one `started` or `responded` model call
  per run. Read APIs still query up to two candidates and fail closed on
  ambiguity.
- A response consumed by multiple tool ordinals is represented as one
  `tool_batch` consumption. A different consumption kind or event conflicts.
- Final-answer consumption and `RunCompleted` are committed in one transaction
  through `commit_final_answer`; there is no response-consumed/active-run crash
  window.
- Migration backfill accepts legal legacy arbitrary model-call IDs by assigning
  their step from first occurrence. A response without a historical start is
  retained as `legacy-unknown`; multiple active legacy calls fail closed.
- The unfinished-tool query uses `tool_calls_by_run_state` directly and then
  validates the one candidate response event. It does not introduce a join and
  sort that would require a temporary B-tree.
- Terminal and phase transitions were included in the optimization scope after
  review found they still invoked full reduction internally.

## Persistence and migration

Schema migration 4 adds `model_calls` and
`operational_projection_migrations`. Relevant model events update the immutable
event row and operational projection inside the same SQLite transaction.
Identity, step, model, attempt ordering, causation, response blob, and
consumption conflicts all fail closed and roll the event insertion back.

Initialization performs a one-time, ordered, payload-validated backfill. The
completion marker is written in the same `BEGIN IMMEDIATE` transaction; an
injected failure leaves neither partial projection rows nor a completion marker,
so the next initialization retries safely.

## Runtime and recovery semantics

- `get_unfinished_tool_call`: reads at most two indexed candidate rows and
  rejects ambiguity.
- `get_latest_unconsumed_model_response`: reads at most two pending model rows,
  then validates the referenced event and blob hash.
- `get_next_model_step`: reads the latest indexed step; a started call resumes
  the same step, while a responded/consumed call advances.
- `get_model_call`: provides `latest_attempt_no` without scanning attempts.
- `commit_final_answer`: atomically consumes the model response, appends final
  and completed events, updates run/turn terminal state, and advances execution
  context.

`runtime/agent.py` contains no `load_run_events` call. The bounded 64-event
prompt working set and 12 transcript-line limit remain unchanged.

## Query-plan evidence

`tests/test_runtime_operational_projections.py` executes
`EXPLAIN QUERY PLAN` and fails if the critical statements contain a table scan
or temporary B-tree. It verifies use of:

- `tool_calls_by_run_state`;
- `model_calls_by_run_step`;
- `model_calls_pending_response`;
- the events primary-key auto-index.

## Local paired benchmark

Environment recorded in each raw JSON report: Windows 11, Python 3.13.12,
SQLite 3.50.4, WAL, `synchronous=FULL`, Intel Family 6 Model 183. Each pair
alternates randomized legacy/indexed execution order after warmup. The baseline
functions freeze the removed Python full-history scans; raw samples are retained.

| History | Samples | Query | Legacy P50 | Indexed P50 | P50 speedup |
|---:|---:|---|---:|---:|---:|
| 10,000 events | 100 | Unfinished tool | 301.55 ms | 4.63 ms | 65.2x |
| 10,000 events | 100 | Pending response | 245.58 ms | 3.98 ms | 61.6x |
| 10,000 events | 100 | Next model step | 665.94 ms | 7.99 ms | 83.4x |
| 10,000 events | 100 | Attempt offset | 659.36 ms | 7.92 ms | 83.3x |
| 100,000 events | 20 | Unfinished tool | 1,786.77 ms | 2.67 ms | 670.0x |
| 100,000 events | 20 | Pending response | 1,444.58 ms | 2.24 ms | 645.5x |
| 100,000 events | 20 | Next model step | 1,373.14 ms | 1.84 ms | 746.8x |
| 100,000 events | 20 | Attempt offset | 1,354.86 ms | 1.90 ms | 714.8x |

Raw reports:

- `benchmarks/results/runtime-hot-path-v05-10k.json`
- `benchmarks/results/runtime-hot-path-v05-100k.json`

These are local SQLite store-call microbenchmarks, not end-to-end Agent latency,
multi-host capacity, or a production SLA. The 100,000-event group uses 20
paired samples because the frozen legacy scans dominate wall time; P95 and every
raw observation are still reported.

## Verification

- `uv run pytest -q`: 223 passed, 4 skipped.
- `uv run ruff check .`: passed.
- Pyright on the three changed production/benchmark modules: 0 errors, 0
  warnings. Full-repository Pyright is not a clean gate yet because the project
  already contains unrelated type debt and no configured Pyright dependency.
- Runtime hot-path/query-plan tests: 10 passed.
- Checkpoint recovery tests: 9 passed, including lazy newest-valid selection,
  corrupt-newest fallback, and all-invalid full replay.

## Remaining boundaries

- The projections are caches of event truth; offline audit/rebuild still scans
  history by design.
- SQLite remains a single-host implementation. This change does not provide a
  distributed cache, remote queue, or cross-database transaction.
- A valid checkpoint bounds tail replay; an all-corrupt checkpoint set must do
  a full replay to preserve correctness.
- The benchmark isolates storage decisions. Real model latency and tool
  execution remain dominant in ordinary short runs.

