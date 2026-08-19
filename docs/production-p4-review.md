# ForgeReplay Production P4 Review

Review date: 2026-08-19

## Decision

The P4 model-governance and release-gate core is accepted.

## Implemented

- Tenant policy constrains Provider and region before routing or fallback.
- Hierarchical Run/User/Team/time-window budgets reserve worst-case cost before
  a call and settle from versioned price-book usage.
- Unknown provider outcomes charge the conservative reservation rather than
  silently refunding potentially consumed budget.
- Provider health tracks failure streaks, opens a circuit and enforces a retry
  amplification budget.
- Receipts persist route, region, attempts, usage, cost and Price Book version.
- Shadow/Canary gates jointly enforce sample denominator, task quality,
  recovery safety, duplicate effects, approval bypasses, retry ratio, cost,
  latency and infrastructure validity.

## Verification and boundary

Tests cover policy-safe fallback, hierarchical rejection before physical calls,
unknown-cost settlement and fail-closed release gates. Provider-specific RPM/TPM
and live 429/5xx drills require deployment adapters; the core does not claim a
200-task production benchmark merely because the gate implementation exists.
