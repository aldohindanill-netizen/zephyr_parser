-- High-level sync health snapshot for queue/writeback.

-- 1) Queue status distribution.
SELECT
  status,
  COUNT(*) AS queue_count,
  MIN(created_at) AS oldest_created_at,
  MAX(updated_at) AS latest_updated_at
FROM sync_queue
GROUP BY status
ORDER BY queue_count DESC, status;

-- 2) test_results sync_state distribution.
SELECT
  sync_state,
  COUNT(*) AS results_count,
  MAX(updated_at) AS latest_updated_at
FROM test_results
GROUP BY sync_state
ORDER BY results_count DESC, sync_state;

-- 3) Writeback audit summary for last 24h.
SELECT
  COUNT(*) AS total_attempts_24h,
  COUNT(*) FILTER (WHERE success) AS success_attempts_24h,
  COUNT(*) FILTER (
    WHERE NOT success
       OR response_status IS NULL
       OR response_status < 200
       OR response_status >= 300
  ) AS failed_attempts_24h,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE success)
    / NULLIF(COUNT(*), 0),
    2
  ) AS success_rate_pct_24h,
  MAX(executed_at) AS latest_attempt_at
FROM sync_audit
WHERE executed_at >= NOW() - INTERVAL '24 hours';
