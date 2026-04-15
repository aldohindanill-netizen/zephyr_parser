-- Recent sync errors since cutover timestamp.
-- Edit cutover_ts in params CTE before running.

WITH params AS (
  SELECT
    TIMESTAMPTZ '2026-04-15 07:00:00+00' AS cutover_ts,
    ARRAY['res-demo-1']::text[] AS excluded_test_result_ids
)
SELECT
  a.executed_at,
  a.test_result_id,
  a.response_status,
  a.success,
  q.status AS queue_status,
  q.attempt_count,
  q.last_error,
  LEFT(COALESCE(a.response_body, ''), 500) AS response_body_head
FROM sync_audit a
LEFT JOIN sync_queue q
  ON q.id = a.queue_id
CROSS JOIN params p
WHERE a.executed_at >= p.cutover_ts
  AND a.test_result_id <> ALL (p.excluded_test_result_ids)
  AND (
       a.success = FALSE
    OR a.response_status IS NULL
    OR a.response_status < 200
    OR a.response_status >= 300
  )
ORDER BY a.executed_at DESC
LIMIT 50;
