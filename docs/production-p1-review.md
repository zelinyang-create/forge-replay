# ForgeReplay Production P1 Review

Review date: 2026-08-19

## Decision

The P1 code boundary is accepted. ForgeReplay now contains a fail-closed
OCI/gVisor execution provider rather than treating Git Worktree or host process
supervision as a production sandbox.

## Implemented

- Per-run sandbox specifications pin an immutable image digest and require a
  non-root user, default-deny networking, CPU, memory, PID and wall-clock limits.
- The OCI provider creates read-only containers with `runsc`, drops all Linux
  capabilities, enables `no-new-privileges`, uses a bounded tmpfs and mounts only
  the run workspace.
- Runtime attestation validates the observed handler, image, user, RootFS,
  network, privilege and security options. Any mismatch destroys the container
  and fails closed.
- Exec requests are argv-only, constrained to `/workspace`, time bounded and
  return hashed stdout/stderr receipts.
- The host provider explicitly refuses production construction.
- Signed policy bundles separate deterministic tool policy from model output;
  missing identity, dangerous executables and inline interpreter code fail
  closed, while network and package installation require approval.

## Verification

- Deterministic transport tests verify the exact container security arguments,
  attestation rejection and cleanup behavior.
- Policy tests cover allow, approval and deny decisions plus missing identity.
- The full existing P0 suite remains green.

## Deployment boundary

This Windows development host cannot prove a live Linux `runsc`, cgroup,
seccomp or network-namespace deployment. The provider is executable against a
Linux Docker installation configured with gVisor, but a release environment
must still run the documented live canary/attestation suite. Passing the local
tests is evidence of control-plane semantics, not proof of kernel isolation.
