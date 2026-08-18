# ForgeReplay v0.3 Production P0 Implementation Review

Review date: 2026-08-18  
Baseline: `v0.2.0` (`d68bff9`)  
Implementation branch: `codex/production-p0`

## Executive conclusion

The P0 single-host consistency upgrade is complete. ForgeReplay now has a
defensible durable execution core: worker mutations are fenced by lease epoch
and event-stream version, user control commands are independently idempotent,
physical model retries are auditable, interrupted model responses and budget
reservations resume without a second logical call, and checkpoints are part of
the real projection hot path.

The accurate product claim is **production-oriented, single-host durable Coding
Agent Harness**. It is materially beyond a toy agent and suitable for a strong
portfolio demonstration, but it is not yet a production multi-tenant service.
Untrusted code still needs an OS-level sandbox; distributed workers still need
PostgreSQL/queue semantics, tenant authorization, secret brokering,
observability, operational runbooks, and disaster-recovery exercises.

## Implemented increments

| Work package | Result | Evidence |
|---|---|---|
| Stable boundaries | Runtime, workspace, runner, queue, policy, blob and ledger protocols separate semantics from SQLite/local adapters | `src/forge_replay/ports.py` |
| Worker fencing | `ExecutionContext` carries run, worker, lease epoch, expiry and stream version; all execution-time mutations compare them transactionally | `0092d38`, `tests/test_execution_fencing.py` |
| Control plane | Approval/cancellation use actor, stable command ID and expected stream version; exact retries return the original event | `de6b50b`, schema migration 3 |
| Model attempts | Every physical provider attempt emits a start and, on failure, a classified failure linked by causation | `b2dcde9`, provider/runtime tests |
| Crash recovery | Logical model-call and budget IDs are deterministic; a response persisted before a crash is consumed and settled once on resume | `test_resume_consumes_durable_model_response_and_settles_budget` |
| Checkpoint hot path | Projection reads use newest valid checkpoint plus tail, fall back through corrupt snapshots, and automatically checkpoint stable runtime boundaries | `859ba79`, checkpoint tests |
| Bounded context | Prompt assembly reads at most the latest 64 events and selects the latest 12 relevant transcript entries | runtime prompt tests and store working-set test |
| Stress/evidence | Reproducible stale-epoch and end-to-end SQLite recovery suites save exact denominators, environment and source commit | `src/forge_replay/eval/` and `benchmarks/results/production-p0-*.json` |

## Quantitative results

The following are local measurements on Windows 11 / Python 3.13.12. They are
not production SLAs.

1. Lease fencing stress (`323ad8b`): 10,000 stale writes after an epoch takeover,
   10,000 rejected, 0 accepted. Ten current-epoch control writes committed and
   all ten were durable. The suite processed 87.67 attempted stale transactions
   per second; this number includes projection reads and rollback overhead and
   is not a capacity benchmark.
2. SQLite recovery (`323ad8b`): with 2,000 pre-checkpoint events and a 50-event
   tail across 20 paired iterations, P50 projection recovery fell from 125.35 ms
   to 8.57 ms; P95 fell from 133.15 ms to 10.37 ms; P50 speedup was 14.63x.
3. Existing file crash conformance: 24/24 hardened runs reached a safe terminal
   state across two deterministic crash windows with 0 duplicate file effects;
   the baseline adapter reached 0/24 safe terminals.
4. Existing real-model Held-out result: Bailian `qwen3-coder-plus` passed 14/24
   runs over 8 frozen tasks x 3 repeats. This measures coding ability and must
   remain separate from Harness reliability measurements.

## Invariants reviewed

- A stale or expired worker cannot append an event, change run phase, reserve or
  settle budget, propose/dispatch/finish a tool, checkpoint, or terminate a run.
- A live worker detects control-plane stream changes before its next mutation.
- A repeated control command cannot change its actor or semantic payload.
- Model-call attempts have one logical identity across provider retries and
  process restart.
- Budget reservation and settlement are idempotent; resume does not create a
  second logical model-call charge.
- Checkpoints are disposable caches. A checksum, schema or content failure
  causes fallback; append-only events remain the source of truth.
- File writes remain detectable/reconcilable. Arbitrary shell outcomes remain
  `UNCERTAIN` when the crash window cannot be proven; exactly-once is not claimed.

## Review findings and limits

No release-blocking defect remained in the deterministic P0 scope after the
final review. Two edge cases found during review were fixed: new checkpoints
now build from the latest valid snapshot instead of replaying the full stream,
and unexpected provider exceptions are attached to the exact latest physical
attempt.

The following are explicit limitations, not hidden backlog:

| Priority | Remaining boundary | Required next step |
|---|---|---|
| P0 before untrusted repositories | Worktree is not a security sandbox | Ephemeral VM/container, low-privilege identity, no mounted secrets, default-deny egress, resource quotas |
| P1 before multiple workers/hosts | SQLite is a single-host truth store and there is no durable remote queue | PostgreSQL event/lease store plus at-least-once queue adapter and multi-process takeover soak |
| P1 before external users | Local CLI actor strings are not authenticated identities | API service, tenant-scoped authorization, signed control-command identity and audit retention |
| P1 operations | No OpenTelemetry export, SLO dashboard, alerting or recovery runbook automation | Low-cardinality metrics, trace export, kill switch, backup/restore and incident drills |
| P1 secret safety | Model credentials are process environment configuration | Short-lived secret broker, provider allowlist, redaction tests and egress policy |
| P2 long sessions | A fixed recent-event window can omit old semantic context | Durable summary/memory projection with versioned compaction and loss tests |
| P2 model adapters | Attempt visibility relies on adapters honoring `ModelAttemptObserver` | Adapter conformance certification or transport-level interception |

## Release decision

P0 is accepted for a single user, trusted repository and trusted host. The code
must not be marketed as a secure hosted Coding Agent or run arbitrary
model-generated processes on a credentialed workstation. The next implementation
milestone should be the isolated runner boundary, followed by PostgreSQL plus a
durable queue; adding multi-Agent orchestration before those two layers would
increase risk without improving the core production claim.

## Final validation

- `pytest`: 189 passed, 3 skipped in 61.88 seconds.
- Ruff: all checks passed.
- Source distribution and wheel: `forge-replay 0.3.0` built successfully.
- Git whitespace validation: no errors.
- Increment from `v0.2.0` through the P0 code review: 7 scoped implementation
  commits before the release/evidence commit; benchmark JSON files preserve the
  exact source commit used for each measurement.
