-- Recent writeback failures and non-2xx responses.
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
WHERE
      a.success = FALSE
   OR a.response_status IS NULL
   OR a.response_status < 200
   OR a.response_status >= 300
ORDER BY a.executed_at DESC
LIMIT 20;
