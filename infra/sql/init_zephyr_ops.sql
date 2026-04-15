BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS folders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    folder_id TEXT NOT NULL UNIQUE,
    folder_name TEXT NOT NULL DEFAULT '',
    folder_path TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_ingested_at TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS test_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_run_id TEXT NOT NULL UNIQUE,
    folder_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL DEFAULT '',
    run_name TEXT NOT NULL DEFAULT '',
    execution_day DATE NULL,
    source_status_name TEXT NOT NULL DEFAULT '',
    last_ingested_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_test_runs_folder_id
    ON test_runs(folder_id);

CREATE TABLE IF NOT EXISTS test_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_result_id TEXT NOT NULL UNIQUE,
    test_run_id TEXT NOT NULL,
    test_case_key TEXT NOT NULL DEFAULT '',
    test_case_name TEXT NOT NULL DEFAULT '',
    current_status_name TEXT NOT NULL DEFAULT '',
    desired_status_name TEXT NULL,
    desired_status_id TEXT NULL,
    desired_comment TEXT NULL,
    last_synced_status_name TEXT NULL,
    last_synced_comment TEXT NULL,
    sync_state TEXT NOT NULL DEFAULT 'pending',
    sync_error TEXT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_test_results_test_run_id
    ON test_results(test_run_id);

CREATE INDEX IF NOT EXISTS idx_test_results_sync_state
    ON test_results(sync_state);

ALTER TABLE test_results
    ADD COLUMN IF NOT EXISTS test_case_name TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS grist_folder_docs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    folder_id TEXT NOT NULL UNIQUE,
    grist_doc_id TEXT NOT NULL,
    grist_table_id TEXT NOT NULL DEFAULT 'cases',
    grist_doc_name TEXT NOT NULL DEFAULT '',
    grist_doc_url TEXT NOT NULL DEFAULT '',
    last_synced_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_grist_folder_docs_doc_id
    ON grist_folder_docs(grist_doc_id);

CREATE TABLE IF NOT EXISTS sync_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_result_id TEXT NOT NULL,
    operation_type TEXT NOT NULL DEFAULT 'writeback',
    operation_hash TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'queued',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NULL,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (test_result_id, operation_hash)
);

CREATE INDEX IF NOT EXISTS idx_sync_queue_status_retry
    ON sync_queue(status, next_retry_at);

CREATE TABLE IF NOT EXISTS sync_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id UUID NULL,
    test_result_id TEXT NOT NULL,
    request_method TEXT NOT NULL DEFAULT '',
    request_url TEXT NOT NULL DEFAULT '',
    request_body_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_status INTEGER NULL,
    response_body TEXT NULL,
    success BOOLEAN NOT NULL DEFAULT FALSE,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_audit_test_result_id
    ON sync_audit(test_result_id);

CREATE OR REPLACE FUNCTION enqueue_sync_queue_on_desired_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    op_hash TEXT;
    payload JSONB;
BEGIN
    IF NEW.desired_status_id IS NULL
       AND (NEW.desired_comment IS NULL OR BTRIM(NEW.desired_comment) = '') THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE'
       AND NEW.desired_status_id IS NOT DISTINCT FROM OLD.desired_status_id
       AND NEW.desired_comment IS NOT DISTINCT FROM OLD.desired_comment THEN
        RETURN NEW;
    END IF;

    payload := jsonb_build_object(
        'desired_status_id', NEW.desired_status_id,
        'desired_comment', COALESCE(NEW.desired_comment, '')
    );

    op_hash := md5(
        NEW.test_result_id
        || '|'
        || COALESCE(NEW.desired_status_id, '')
        || '|'
        || COALESCE(NEW.desired_comment, '')
        || '|'
        || to_char(clock_timestamp(), 'YYYYMMDDHH24MISSUS')
    );

    INSERT INTO sync_queue (
        test_result_id,
        operation_type,
        operation_hash,
        payload_json,
        status
    )
    VALUES (
        NEW.test_result_id,
        'writeback',
        op_hash,
        payload,
        'queued'
    )
    ON CONFLICT (test_result_id, operation_hash) DO NOTHING;

    NEW.sync_state := 'pending';
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_enqueue_sync_queue_on_change ON test_results;
CREATE TRIGGER trg_enqueue_sync_queue_on_change
BEFORE INSERT OR UPDATE OF desired_status_id, desired_comment
ON test_results
FOR EACH ROW
EXECUTE FUNCTION enqueue_sync_queue_on_desired_change();

DROP VIEW IF EXISTS operator_daily_form;
DROP VIEW IF EXISTS operator_day_summary;
DROP VIEW IF EXISTS operator_run_form;

CREATE OR REPLACE VIEW operator_run_form AS
SELECT
    tr.test_result_id,
    COALESCE(NULLIF(ru.run_name, ''), NULLIF(f.folder_name, ''), 'unassigned') AS scenario,
    tr.test_case_name,
    tr.test_case_key,
    COALESCE(NULLIF(tr.test_case_key, ''), NULLIF(tr.test_case_name, ''), tr.test_result_id) AS resolved_case_key,
    tr.test_run_id,
    COALESCE(NULLIF(ru.cycle_id, ''), tr.test_run_id) AS execution_id,
    ru.run_name,
    ru.execution_day,
    ru.folder_id,
    f.folder_name,
    f.folder_path,
    tr.current_status_name,
    tr.desired_status_name,
    tr.desired_status_id,
    tr.desired_comment,
    -- Adjust IDs 145/146 to match your Zephyr Pass/Fail status IDs
    (tr.desired_status_id = '145') AS pass_checked,
    (tr.desired_status_id = '146') AS fail_checked,
    tr.sync_state,
    tr.sync_state AS sync_status,
    tr.sync_error,
    tr.last_synced_status_name,
    tr.last_synced_comment,
    CASE
        WHEN tr.sync_state = 'done' THEN tr.updated_at
        ELSE NULL
    END AS synced_at,
    tr.updated_at
FROM test_results tr
LEFT JOIN test_runs ru ON ru.test_run_id = tr.test_run_id
LEFT JOIN folders f ON f.folder_id = ru.folder_id;

CREATE OR REPLACE VIEW operator_day_summary AS
SELECT
    ru.execution_day,
    ru.folder_id,
    f.folder_name,
    COUNT(*)                                                    AS total_cases,
    COUNT(*) FILTER (WHERE tr.desired_status_id IS NOT NULL)    AS with_desired_status,
    COUNT(*) FILTER (WHERE tr.sync_state = 'pending')           AS pending_sync,
    COUNT(*) FILTER (WHERE tr.sync_state = 'done')              AS done_sync,
    COUNT(*) FILTER (WHERE tr.sync_state = 'failed'
                        OR tr.sync_state = 'dead_letter')       AS failed_sync,
    MAX(tr.updated_at)                                          AS last_updated_at
FROM test_results tr
LEFT JOIN test_runs ru ON ru.test_run_id = tr.test_run_id
LEFT JOIN folders f ON f.folder_id = ru.folder_id
WHERE ru.execution_day IS NOT NULL
GROUP BY ru.execution_day, ru.folder_id, f.folder_name
ORDER BY ru.execution_day DESC, f.folder_name;

CREATE OR REPLACE VIEW operator_daily_form AS
SELECT
    rf.*
FROM operator_run_form rf
WHERE rf.execution_day >= CURRENT_DATE - INTERVAL '7 days';

COMMIT;
