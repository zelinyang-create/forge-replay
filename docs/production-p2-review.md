# ForgeReplay Production P2 Review

Review date: 2026-08-19

## Decision

The P2 control-plane implementation is accepted at code level. PostgreSQL is
the authoritative state store; the durable queue only wakes workers and the
transactional outbox carries external notifications.

## Implemented

- PostgreSQL schema for tenant-scoped runs, run-local event streams, commands,
  outbox, API idempotency, artifacts and references.
- Row-Level Security policies use transaction-local tenant context.
- Run creation atomically commits the Run, first Event, start Command, Outbox
  message and Idempotency response.
- Stream advancement uses expected-version CAS and can commit Event, Command
  and Outbox in the same short transaction.
- Workers claim commands with `FOR UPDATE SKIP LOCKED`; acknowledgement is
  owner-bound and delivery is explicitly at least once.
- Artifact bytes are fsynced, verified and atomically published into a
  tenant-scoped CAS before PostgreSQL records a reference.
- The FastAPI control plane requires a signed identity and Idempotency-Key and
  derives tenant scope from the verified principal.

## Verification and boundary

Schema, API authorization/idempotency and tenant CAS tests run locally. A real
PostgreSQL integration test activates when `FORGE_REPLAY_TEST_POSTGRES_DSN` is
configured. This host has no PostgreSQL service, so no claim is made for
multi-process load, deployment-role RLS, PITR or failover; those remain release
environment gates rather than silently skipped guarantees.
