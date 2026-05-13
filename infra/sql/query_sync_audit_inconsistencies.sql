-- Detect inconsistent audit rows where success flag conflicts with HTTP status.
-- Expected rule: success=true only for 2xx statuses.

SELECT
  a.id::text AS audit_id,
  a.executed_at,
  a.test_result_id,
  a.response_status,
  a.success,
  q.status AS queue_status,
  q.attempt_count,
  LEFT(COALESCE(a.response_body, ''), 300) AS response_body_head
FROM sync_audit a
LEFT JOIN sync_queue q
  ON q.id = a.queue_id
WHERE
      (a.success = TRUE  AND (a.response_status IS NULL OR a.response_status < 200 OR a.response_status >= 300))
   OR (a.success = FALSE AND  a.response_status >= 200 AND a.response_status < 300)
ORDER BY a.executed_at DESC
LIMIT 200;
