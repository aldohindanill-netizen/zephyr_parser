# Production cutover checklist

Use this checklist when moving from local validation to production operation.

## Pre-cutover

- Confirm `zephyr_ingest_15m` and `zephyr_writeback_15m` latest manual executions are `success`.
- Validate queue health queries:
  - `infra/sql/query_sync_health.sql`
  - `infra/sql/query_sync_recent_errors.sql`
  - `infra/sql/query_sync_sla.sql`
- Verify no stale queue backlog:
  - `queued_now = 0` or expected small value
  - no unexpected `dead_letter` growth
- Validate `sync_audit.success` consistency:
  - run `infra/sql/query_sync_audit_inconsistencies.sql`
  - if needed, normalize historical rows with `infra/sql/fix_sync_audit_success_flag.sql`
- Confirm production URLs/env:
  - `N8N_HOST`, `N8N_PROTOCOL=https`, `N8N_WEBHOOK_URL`
  - `NOCODB_PUBLIC_URL`
  - `ZEPHYR_BASE_URL`, `ZEPHYR_API_TOKEN`
- Confirm only one active workflow per logical function:
  - one `zephyr_ingest_15m`
  - one `zephyr_writeback_15m`
- Verify n8n credential `PostgresZephyrOps` works in production network.

## Local baseline before prod

Run the cutover-scoped SQL set and keep the output in the release notes:

- `infra/sql/query_sync_health_since_cutover.sql`
- `infra/sql/query_sync_recent_errors_since_cutover.sql`

Recommended readiness gates before moving to production:

- `sync_queue` has no sustained backlog (`queued` drains back to 0 after a cycle).
- no new `dead_letter` rows for controlled smoke scenarios.
- `sync_audit` success rate is stable (target >= 95% for smoke window, >= 99% for
  steady-state volume).
- every non-2xx response has a matching queue state transition to `failed` or
  `dead_letter` (never silently `done`).

Current local snapshot (2026-04-15, cutover `07:00:00+00`):

- queue statuses: `done=2`, `queued=1`
- sync attempts: `total=2`, `success=1`, `failed=1`, `success_rate_pct=50.00`
- recent non-2xx sample: one `401` audit row captured with response body

Conclusion: baseline is useful for validation, but not yet production-ready until
the queued item is drained and the post-fix success rate is re-measured.

## Cutover steps

1. Deploy production env values (`infra/.env.nocodb-n8n` equivalent in secret manager).
2. Import runtime workflows built from production env:
   - `.zephyr_ingest_15m.runtime.json`
   - `.zephyr_writeback_15m.runtime.json`
3. Run manual smoke:
   - execute ingest once
   - create one controlled queue item and execute writeback once
4. Check DB outcomes:
   - ingest updates `folders`, `test_runs`, `test_results`
   - writeback row reaches `sync_queue.status=done` with `sync_audit.response_status` in 2xx
5. Activate schedules.

## Post-cutover (first 24h)

- Every 1-2 hours review:
  - `query_sync_health.sql`
  - `query_sync_recent_errors.sql`
  - `query_sync_sla.sql`
- Trigger rollback actions if:
  - sustained non-2xx responses
  - growing `dead_letter` rate
  - queue backlog older than SLA threshold

## Rollback

- Disable `zephyr_writeback_15m` immediately.
- Keep ingest active (optional) for observability only.
- Investigate failed payloads via `sync_audit.response_body`.
- Re-queue only corrected operations with fresh `operation_hash`.
