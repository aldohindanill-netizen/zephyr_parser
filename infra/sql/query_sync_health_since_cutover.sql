-- Sync health snapshot since cutover timestamp.
-- Edit cutover_ts in params CTE before running.

WITH params AS (
  SELECT
    TIMESTAMPTZ '2026-04-15 07:00:00+00' AS cutover_ts,
    ARRAY['res-demo-1']::text[] AS excluded_test_result_ids
)
SELECT
  status,
  COUNT(*) AS queue_count,
  MIN(created_at) AS oldest_created_at,
  MAX(updated_at) AS latest_updated_at
FROM sync_queue, params p
WHERE created_at >= p.cutover_ts
  AND test_result_id <> ALL (p.excluded_test_result_ids)
GROUP BY status
ORDER BY queue_count DESC, status;

WITH params AS (
  SELECT
    TIMESTAMPTZ '2026-04-15 07:00:00+00' AS cutover_ts,
    ARRAY['res-demo-1']::text[] AS excluded_test_result_ids
)
SELECT
  sync_state,
  COUNT(*) AS results_count,
  MAX(updated_at) AS latest_updated_at
FROM test_results, params p
WHERE updated_at >= p.cutover_ts
  AND test_result_id <> ALL (p.excluded_test_result_ids)
GROUP BY sync_state
ORDER BY results_count DESC, sync_state;

WITH params AS (
  SELECT
    TIMESTAMPTZ '2026-04-15 07:00:00+00' AS cutover_ts,
    ARRAY['res-demo-1']::text[] AS excluded_test_result_ids
)
SELECT
  COUNT(*) AS total_attempts,
  COUNT(*) FILTER (WHERE success) AS success_attempts,
  COUNT(*) FILTER (
    WHERE NOT success
       OR response_status IS NULL
       OR response_status < 200
       OR response_status >= 300
  ) AS failed_attempts,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE success)
    / NULLIF(COUNT(*), 0),
    2
  ) AS success_rate_pct,
  MAX(executed_at) AS latest_attempt_at
FROM sync_audit, params p
WHERE executed_at >= p.cutover_ts
  AND test_result_id <> ALL (p.excluded_test_result_ids);
