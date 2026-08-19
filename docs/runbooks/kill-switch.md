# Kill Switch and Queue Drain Runbook

1. Reject new Run admission while keeping status and artifact reads available.
2. Stop workers from claiming new Commands; allow bounded in-flight work to
   finish or cancel it using persisted control commands.
3. Revoke model, SCM and sandbox capability leases for the affected scope.
4. Reconcile claimed Commands, unknown Tool Attempts, budget reservations,
   orphan Sandboxes and Outbox lag.
5. Require a release-gate decision and incident owner approval before reopening.
