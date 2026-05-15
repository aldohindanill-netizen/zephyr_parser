-- Offline validation for external Pass/Fail form pipeline.
-- Works without VPN/Jira connectivity.
--
-- Usage:
-- 1) Replace the test_result_id literal in params CTE.
-- 2) Run this script against zephyr_ops Postgres.

WITH params AS (
  SELECT '112522'::text AS test_result_id
)
SELECT
  tr.test_result_id,
  ru.folder_id,
  ru.run_name,
  tr.test_case_name,
  tr.desired_status_id,
  CASE
    WHEN tr.desired_status_id = '145' THEN 'pass'
    WHEN tr.desired_status_id = '146' THEN 'fail'
    WHEN tr.desired_status_id IS NULL OR BTRIM(tr.desired_status_id) = '' THEN 'empty'
    ELSE 'unexpected_status_id'
  END AS desired_status_validation,
  tr.desired_comment,
  tr.sync_state,
  tr.sync_error,
  tr.updated_at
FROM test_results tr
LEFT JOIN test_runs ru ON ru.test_run_id = tr.test_run_id
WHERE tr.test_result_id = (SELECT test_result_id FROM params)
ORDER BY tr.updated_at DESC
LIMIT 10;

WITH params AS (
  SELECT '112522'::text AS test_result_id
)
SELECT
  sq.id,
  sq.test_result_id,
  sq.status,
  sq.attempt_count,
  sq.payload_json ->> 'desired_status_id' AS queued_status_id,
  sq.payload_json ->> 'desired_comment' AS queued_comment,
  sq.last_error,
  sq.created_at,
  sq.updated_at
FROM sync_queue sq
WHERE sq.test_result_id = (SELECT test_result_id FROM params)
ORDER BY sq.created_at DESC
LIMIT 10;

WITH params AS (
  SELECT '112522'::text AS test_result_id
)
SELECT
  sa.id,
  sa.test_result_id,
  sa.success,
  sa.response_status,
  sa.executed_at
FROM sync_audit sa
WHERE sa.test_result_id = (SELECT test_result_id FROM params)
ORDER BY sa.executed_at DESC
LIMIT 10;
