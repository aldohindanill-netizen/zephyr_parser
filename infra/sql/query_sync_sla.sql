-- SLA-style operational metrics for Zephyr sync.
-- Focus window: last 24 hours.

-- 1) Queue processing throughput by terminal status.
SELECT
  status,
  COUNT(*) AS queue_count_24h
FROM sync_queue
WHERE updated_at >= NOW() - INTERVAL '24 hours'
GROUP BY status
ORDER BY queue_count_24h DESC, status;

-- 2) Backlog health: queued items and age.
SELECT
  COUNT(*) FILTER (WHERE status = 'queued') AS queued_now,
  COUNT(*) FILTER (
    WHERE status = 'queued'
      AND created_at <= NOW() - INTERVAL '15 minutes'
  ) AS queued_older_than_15m,
  MIN(created_at) FILTER (WHERE status = 'queued') AS oldest_queued_created_at
FROM sync_queue;

-- 3) Queue-to-attempt latency (first attempt) percentiles.
WITH first_attempt AS (
  SELECT
    a.queue_id,
    MIN(a.executed_at) AS first_executed_at
  FROM sync_audit a
  WHERE a.executed_at >= NOW() - INTERVAL '24 hours'
    AND a.queue_id IS NOT NULL
  GROUP BY a.queue_id
),
latency AS (
  SELECT
    EXTRACT(EPOCH FROM (f.first_executed_at - q.created_at)) AS latency_seconds
  FROM first_attempt f
  JOIN sync_queue q ON q.id = f.queue_id
)
SELECT
  COUNT(*) AS sampled_queues,
  ROUND(AVG(latency_seconds)::numeric, 2) AS avg_latency_sec,
  ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_seconds)::numeric, 2) AS p50_latency_sec,
  ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_seconds)::numeric, 2) AS p95_latency_sec,
  ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_seconds)::numeric, 2) AS p99_latency_sec
FROM latency;

-- 4) Retry and dead-letter rates over updated queue rows.
WITH updated_rows AS (
  SELECT *
  FROM sync_queue
  WHERE updated_at >= NOW() - INTERVAL '24 hours'
)
SELECT
  COUNT(*) AS total_updated_rows_24h,
  COUNT(*) FILTER (WHERE attempt_count > 0) AS rows_with_retries_24h,
  COUNT(*) FILTER (WHERE status = 'dead_letter') AS dead_letter_rows_24h,
  ROUND(100.0 * COUNT(*) FILTER (WHERE attempt_count > 0) / NULLIF(COUNT(*), 0), 2) AS retry_rate_pct_24h,
  ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'dead_letter') / NULLIF(COUNT(*), 0), 2) AS dead_letter_rate_pct_24h
FROM updated_rows;
