# Database and Region Failover Runbook

1. Enable the control-plane kill switch and stop new Run admission.
2. Fence the old regional generation and preserve its audit anchor.
3. Restore PostgreSQL to the approved recovery point and verify event counts,
   terminal states, tombstones and artifact references.
4. Verify object-store index hashes, KMS, policy bundles and runner capacity.
5. Produce a signed `RegionEvidence` bundle and promote with expected-generation
   CAS. A stale generation must stop the procedure.
6. Resume synthetic Runs before user traffic; watch queue age, outbox lag,
   duplicate effects and unowned leases.
7. Roll back by fencing the failed generation, never by running two writable
   regions with the same generation.
