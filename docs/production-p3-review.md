# ForgeReplay Production P3 Review

Review date: 2026-08-19

## Decision

The P3 takeover and workspace-recovery protocol is accepted at code level.

## Implemented

- PostgreSQL worker leases increment an epoch on ownership acquisition; worker
  mutations require Tenant, Owner, live Epoch and expected Stream Version.
- Worker takeover terminates stale-epoch sandboxes before attempting new work,
  then reconnects an existing sandbox or restores the latest snapshot.
- Workspace snapshots use a deterministic Tree Manifest, tenant CAS blobs,
  Base SHA binding, modes, symlink checks, path validation and a root hash.
- Restore requires an empty destination and re-captures the tree to prove the
  restored root hash equals the committed snapshot.
- PostgreSQL schema now persists workspace snapshots, sandbox jobs and workers.

## Verification and boundary

Deterministic tests cover exact snapshot restoration, Base SHA rejection,
stale-sandbox termination, reconnect preference and snapshot fallback. A real
multi-host soak still requires the P2 PostgreSQL and P1 gVisor deployment
environment; local tests validate orchestration semantics, not a 72-hour
infrastructure claim.
