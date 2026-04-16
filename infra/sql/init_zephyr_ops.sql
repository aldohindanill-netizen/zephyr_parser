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
    cycle_objective TEXT NOT NULL DEFAULT '',
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

ALTER TABLE test_runs
    ADD COLUMN IF NOT EXISTS cycle_objective TEXT NOT NULL DEFAULT '';

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

CREATE UNIQUE INDEX IF NOT EXISTS uq_grist_folder_docs_doc_table
    ON grist_folder_docs(grist_doc_id, grist_table_id);

CREATE TABLE IF NOT EXISTS baserow_folder_registry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    folder_id TEXT NOT NULL UNIQUE,
    baserow_table_id TEXT NOT NULL,
    baserow_table_name TEXT NOT NULL DEFAULT '',
    baserow_public_view_id TEXT NULL,
    baserow_public_view_url TEXT NOT NULL DEFAULT '',
    baserow_submission_table_id TEXT NULL,
    baserow_submission_table_name TEXT NOT NULL DEFAULT '',
    baserow_submission_form_url TEXT NOT NULL DEFAULT '',
    last_synced_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_baserow_folder_registry_table_id
    ON baserow_folder_registry(baserow_table_id);

CREATE TABLE IF NOT EXISTS webask_scenario_forms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    folder_id TEXT NOT NULL,
    scenario_key TEXT NOT NULL,
    scenario_title TEXT NOT NULL DEFAULT '',
    webask_form_id TEXT NULL,
    webask_form_name TEXT NOT NULL DEFAULT '',
    webask_form_url TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_synced_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (folder_id, scenario_key)
);

CREATE INDEX IF NOT EXISTS idx_webask_scenario_forms_form_id
    ON webask_scenario_forms(webask_form_id);

CREATE TABLE IF NOT EXISTS webask_widget_map (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webask_form_id TEXT NULL,
    folder_id TEXT NOT NULL,
    scenario_key TEXT NOT NULL,
    test_run_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL DEFAULT '',
    test_result_id TEXT NOT NULL,
    test_case_key TEXT NOT NULL DEFAULT '',
    test_case_name TEXT NOT NULL DEFAULT '',
    widget_uuid TEXT NOT NULL,
    field_kind TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (widget_uuid),
    UNIQUE (test_result_id, field_kind)
);

CREATE INDEX IF NOT EXISTS idx_webask_widget_map_lookup
    ON webask_widget_map(webask_form_id, folder_id, scenario_key);

CREATE TABLE IF NOT EXISTS tally_scenario_forms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    folder_id TEXT NOT NULL,
    scenario_key TEXT NOT NULL,
    scenario_title TEXT NOT NULL DEFAULT '',
    tally_form_id TEXT NULL,
    tally_form_name TEXT NOT NULL DEFAULT '',
    tally_form_url TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_synced_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (folder_id, scenario_key)
);

CREATE INDEX IF NOT EXISTS idx_tally_scenario_forms_form_id
    ON tally_scenario_forms(tally_form_id);

CREATE TABLE IF NOT EXISTS operator_result_submissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    folder_id TEXT NOT NULL,
    scenario_key TEXT NOT NULL,
    scenario_title TEXT NOT NULL DEFAULT '',
    cycle_id TEXT NOT NULL DEFAULT '',
    test_run_id TEXT NOT NULL,
    test_result_id TEXT NOT NULL,
    test_case_key TEXT NOT NULL DEFAULT '',
    test_case_name TEXT NOT NULL DEFAULT '',
    selected_outcome TEXT NOT NULL DEFAULT '',
    desired_status_id TEXT NULL,
    desired_comment TEXT NULL,
    baserow_row_id TEXT NULL,
    webask_log_id TEXT NULL,
    tally_submission_id TEXT NULL,
    submission_source TEXT NOT NULL DEFAULT 'baserow',
    webhook_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    processed_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE operator_result_submissions
    ADD COLUMN IF NOT EXISTS tally_submission_id TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_operator_result_submissions_pending
    ON operator_result_submissions(processed_at, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS uq_operator_result_submissions_source_row
    ON operator_result_submissions(submission_source, baserow_row_id)
    WHERE baserow_row_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_operator_result_submissions_webask_log
    ON operator_result_submissions(submission_source, webask_log_id)
    WHERE webask_log_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_operator_result_submissions_tally_submission
    ON operator_result_submissions(submission_source, tally_submission_id)
    WHERE tally_submission_id IS NOT NULL;

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
DROP VIEW IF EXISTS baserow_public_case_form;
DROP VIEW IF EXISTS baserow_scenario_index;
DROP VIEW IF EXISTS webask_case_export;
DROP VIEW IF EXISTS webask_scenario_index;

CREATE OR REPLACE FUNCTION derive_scenario_from_run_name(run_name TEXT)
RETURNS TABLE (
    scenario_key TEXT,
    scenario_title TEXT,
    scenario_order INTEGER,
    cycle_order NUMERIC,
    cycle_suffix TEXT
)
LANGUAGE sql
IMMUTABLE
AS $$
WITH parsed AS (
    SELECT
        COALESCE(run_name, '') AS original_name,
        regexp_match(COALESCE(run_name, ''), '^\s*(\d+)(?:\.(\d+))?\s*(?:[.\-:]|\s)\s*(.+?)\s*$') AS match_parts
)
SELECT
    COALESCE(match_parts[1], 'ungrouped') AS scenario_key,
    CASE
        WHEN match_parts IS NULL THEN NULLIF(BTRIM(original_name), '')
        ELSE CONCAT(match_parts[1], ' ', split_part(COALESCE(match_parts[3], ''), ' возможен ', 1))
    END AS scenario_title,
    CASE
        WHEN match_parts IS NULL THEN 2147483647
        ELSE match_parts[1]::INTEGER
    END AS scenario_order,
    CASE
        WHEN match_parts IS NULL THEN 2147483647::NUMERIC
        WHEN COALESCE(match_parts[2], '') = '' THEN match_parts[1]::NUMERIC
        ELSE (match_parts[1] || '.' || match_parts[2])::NUMERIC
    END AS cycle_order,
    COALESCE(match_parts[2], '') AS cycle_suffix
FROM parsed;
$$;

CREATE OR REPLACE FUNCTION longest_common_prefix(input_values TEXT[])
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    prefix TEXT;
    candidate TEXT;
    value_idx INTEGER;
    char_idx INTEGER;
    max_len INTEGER;
BEGIN
    IF input_values IS NULL OR array_length(input_values, 1) IS NULL THEN
        RETURN '';
    END IF;

    prefix := COALESCE(input_values[1], '');
    IF prefix = '' THEN
        RETURN '';
    END IF;

    FOR value_idx IN 2 .. array_length(input_values, 1) LOOP
        candidate := COALESCE(input_values[value_idx], '');
        IF candidate = '' THEN
            RETURN '';
        END IF;

        max_len := LEAST(length(prefix), length(candidate));
        char_idx := 1;
        WHILE char_idx <= max_len LOOP
            EXIT WHEN substr(prefix, char_idx, 1) <> substr(candidate, char_idx, 1);
            char_idx := char_idx + 1;
        END LOOP;

        prefix := substr(prefix, 1, char_idx - 1);
        IF prefix = '' THEN
            RETURN '';
        END IF;
    END LOOP;

    RETURN prefix;
END;
$$;

CREATE OR REPLACE FUNCTION canonical_scenario_title(scenario_key TEXT, input_values TEXT[])
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    cleaned_titles TEXT[];
    prefix TEXT;
    prefix_tokens TEXT[];
BEGIN
    SELECT ARRAY(
        SELECT DISTINCT regexp_replace(BTRIM(value), '\s+\(cloned\)\s*$', '', 'i')
        FROM unnest(COALESCE(input_values, ARRAY[]::TEXT[])) AS value
        WHERE NULLIF(BTRIM(value), '') IS NOT NULL
        ORDER BY 1
    )
    INTO cleaned_titles;

    IF cleaned_titles IS NULL OR array_length(cleaned_titles, 1) IS NULL THEN
        RETURN COALESCE(NULLIF(BTRIM(scenario_key), ''), 'Ungrouped');
    END IF;

    IF array_length(cleaned_titles, 1) = 1 THEN
        RETURN cleaned_titles[1];
    END IF;

    prefix := longest_common_prefix(cleaned_titles);
    IF prefix = '' THEN
        RETURN cleaned_titles[1];
    END IF;

    IF prefix ~ '\s$' THEN
        prefix := BTRIM(prefix);
    ELSE
        prefix := BTRIM(regexp_replace(prefix, '\S+$', ''));
    END IF;

    prefix_tokens := regexp_split_to_array(prefix, '\s+');
    IF array_length(prefix_tokens, 1) IS NOT NULL AND char_length(prefix_tokens[array_length(prefix_tokens, 1)]) = 1 THEN
        prefix := BTRIM(regexp_replace(prefix, '\s+\S+$', ''));
    END IF;

    IF prefix = '' THEN
        RETURN cleaned_titles[1];
    END IF;

    RETURN prefix;
END;
$$;

CREATE OR REPLACE VIEW operator_run_form AS
SELECT
    tr.test_result_id,
    COALESCE(NULLIF(ds.scenario_title, ''), NULLIF(ru.run_name, ''), NULLIF(f.folder_name, ''), 'unassigned') AS scenario,
    ds.scenario_key,
    tr.test_case_name,
    tr.test_case_key,
    COALESCE(NULLIF(tr.test_case_key, ''), NULLIF(tr.test_case_name, ''), tr.test_result_id) AS resolved_case_key,
    tr.test_run_id,
    COALESCE(NULLIF(ru.cycle_id, ''), tr.test_run_id) AS execution_id,
    ru.run_name,
    ru.cycle_objective,
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
LEFT JOIN folders f ON f.folder_id = ru.folder_id
LEFT JOIN LATERAL derive_scenario_from_run_name(ru.run_name) ds ON TRUE;

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

CREATE OR REPLACE VIEW baserow_scenario_index AS
SELECT
    ru.folder_id,
    f.folder_name,
    f.folder_path,
    ru.execution_day,
    ds.scenario_key,
    COALESCE(NULLIF(ds.scenario_title, ''), NULLIF(ru.run_name, ''), 'Ungrouped') AS scenario_title,
    MIN(ds.scenario_order) AS scenario_order,
    COUNT(DISTINCT ru.test_run_id) AS cycle_count,
    COUNT(tr.test_result_id) AS test_case_count,
    MAX(tr.updated_at) AS last_updated_at
FROM test_runs ru
LEFT JOIN folders f ON f.folder_id = ru.folder_id
LEFT JOIN test_results tr ON tr.test_run_id = ru.test_run_id
LEFT JOIN LATERAL derive_scenario_from_run_name(ru.run_name) ds ON TRUE
GROUP BY
    ru.folder_id,
    f.folder_name,
    f.folder_path,
    ru.execution_day,
    ds.scenario_key,
    COALESCE(NULLIF(ds.scenario_title, ''), NULLIF(ru.run_name, ''), 'Ungrouped');

CREATE OR REPLACE VIEW baserow_public_case_form AS
SELECT
    ru.folder_id,
    f.folder_name,
    f.folder_path,
    ru.execution_day,
    ds.scenario_key,
    COALESCE(NULLIF(ds.scenario_title, ''), NULLIF(ru.run_name, ''), 'Ungrouped') AS scenario_title,
    ds.scenario_order,
    ds.cycle_order,
    COALESCE(NULLIF(ru.cycle_id, ''), ru.test_run_id) AS cycle_ref,
    ru.test_run_id,
    ru.cycle_id,
    ru.run_name AS cycle_name,
    ru.cycle_objective,
    tr.test_result_id,
    tr.test_case_key,
    tr.test_case_name,
    tr.current_status_name,
    tr.desired_status_id,
    tr.desired_comment,
    tr.sync_state,
    tr.updated_at
FROM test_runs ru
JOIN test_results tr ON tr.test_run_id = ru.test_run_id
LEFT JOIN folders f ON f.folder_id = ru.folder_id
LEFT JOIN LATERAL derive_scenario_from_run_name(ru.run_name) ds ON TRUE;

CREATE OR REPLACE VIEW webask_scenario_index AS
SELECT
    ru.folder_id,
    f.folder_name,
    f.folder_path,
    ru.execution_day,
    ds.scenario_key,
    COALESCE(NULLIF(ds.scenario_title, ''), NULLIF(ru.run_name, ''), 'Ungrouped') AS scenario_title,
    MIN(ds.scenario_order) AS scenario_order,
    COUNT(DISTINCT ru.test_run_id) AS cycle_count,
    COUNT(tr.test_result_id) AS test_case_count,
    MAX(tr.updated_at) AS last_updated_at,
    wsf.webask_form_id,
    wsf.webask_form_url
FROM test_runs ru
LEFT JOIN folders f ON f.folder_id = ru.folder_id
LEFT JOIN test_results tr ON tr.test_run_id = ru.test_run_id
LEFT JOIN LATERAL derive_scenario_from_run_name(ru.run_name) ds ON TRUE
LEFT JOIN webask_scenario_forms wsf
    ON wsf.folder_id = ru.folder_id
   AND wsf.scenario_key = ds.scenario_key
   AND wsf.is_active = TRUE
GROUP BY
    ru.folder_id,
    f.folder_name,
    f.folder_path,
    ru.execution_day,
    ds.scenario_key,
    COALESCE(NULLIF(ds.scenario_title, ''), NULLIF(ru.run_name, ''), 'Ungrouped'),
    wsf.webask_form_id,
    wsf.webask_form_url;

CREATE OR REPLACE VIEW webask_case_export AS
SELECT
    ru.folder_id,
    f.folder_name,
    f.folder_path,
    ru.execution_day,
    ds.scenario_key,
    COALESCE(NULLIF(ds.scenario_title, ''), NULLIF(ru.run_name, ''), 'Ungrouped') AS scenario_title,
    ds.scenario_order,
    ds.cycle_order,
    COALESCE(NULLIF(ru.cycle_id, ''), ru.test_run_id) AS cycle_ref,
    ru.test_run_id,
    ru.cycle_id,
    ru.run_name AS cycle_name,
    ru.cycle_objective,
    tr.test_result_id,
    tr.test_case_key,
    tr.test_case_name,
    tr.current_status_name,
    tr.desired_status_id,
    tr.desired_comment,
    tr.sync_state,
    tr.updated_at,
    wsf.webask_form_id,
    wsf.webask_form_url,
    CONCAT(
        COALESCE(wsf.webask_form_url, ''),
        CASE WHEN POSITION('?' IN COALESCE(wsf.webask_form_url, '')) > 0 THEN '&' ELSE '?' END,
        'folder_id=', COALESCE(ru.folder_id, ''),
        '&scenario_key=', COALESCE(ds.scenario_key, ''),
        '&scenario_title=', regexp_replace(COALESCE(ds.scenario_title, ''), '\s+', '%20', 'g'),
        '&test_run_id=', COALESCE(ru.test_run_id, ''),
        '&cycle_id=', COALESCE(ru.cycle_id, ''),
        '&test_result_id=', COALESCE(tr.test_result_id, ''),
        '&test_case_key=', regexp_replace(COALESCE(tr.test_case_key, ''), '\s+', '%20', 'g'),
        '&test_case_name=', regexp_replace(COALESCE(tr.test_case_name, ''), '\s+', '%20', 'g')
    ) AS webask_prefilled_url
FROM test_runs ru
JOIN test_results tr ON tr.test_run_id = ru.test_run_id
LEFT JOIN folders f ON f.folder_id = ru.folder_id
LEFT JOIN LATERAL derive_scenario_from_run_name(ru.run_name) ds ON TRUE
LEFT JOIN webask_scenario_forms wsf
    ON wsf.folder_id = ru.folder_id
   AND wsf.scenario_key = ds.scenario_key
   AND wsf.is_active = TRUE;

CREATE OR REPLACE VIEW tally_scenario_index AS
WITH base AS (
    SELECT
        ru.folder_id,
        f.folder_name,
        f.folder_path,
        ru.execution_day,
        ds.scenario_key,
        COALESCE(NULLIF(ds.scenario_title, ''), NULLIF(ru.run_name, ''), 'Ungrouped') AS raw_scenario_title,
        ds.scenario_order,
        ru.test_run_id,
        tr.test_result_id,
        tr.updated_at
    FROM test_runs ru
    LEFT JOIN folders f ON f.folder_id = ru.folder_id
    LEFT JOIN test_results tr ON tr.test_run_id = ru.test_run_id
    LEFT JOIN LATERAL derive_scenario_from_run_name(ru.run_name) ds ON TRUE
),
canonical AS (
    SELECT
        folder_id,
        MAX(folder_name) AS folder_name,
        MAX(folder_path) AS folder_path,
        MAX(execution_day) AS execution_day,
        scenario_key,
        canonical_scenario_title(
            scenario_key,
            array_agg(DISTINCT raw_scenario_title ORDER BY raw_scenario_title)
        ) AS scenario_title,
        MIN(scenario_order) AS scenario_order,
        COUNT(DISTINCT test_run_id) AS cycle_count,
        COUNT(test_result_id) AS test_case_count,
        MAX(updated_at) AS last_updated_at
    FROM base
    GROUP BY folder_id, scenario_key
)
SELECT
    c.folder_id,
    c.folder_name,
    c.folder_path,
    c.execution_day,
    c.scenario_key,
    c.scenario_title,
    c.scenario_order,
    c.cycle_count,
    c.test_case_count,
    c.last_updated_at,
    tsf.tally_form_id,
    tsf.tally_form_url
FROM canonical c
LEFT JOIN tally_scenario_forms tsf
    ON tsf.folder_id = c.folder_id
   AND tsf.scenario_key = c.scenario_key
   AND tsf.is_active = TRUE;

CREATE OR REPLACE FUNCTION url_encode_utf8(input_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    encoded TEXT := '';
    input_bytes BYTEA;
    current_byte INTEGER;
    idx INTEGER;
BEGIN
    IF input_value IS NULL THEN
        RETURN '';
    END IF;

    input_bytes := convert_to(input_value, 'UTF8');

    IF length(input_bytes) = 0 THEN
        RETURN '';
    END IF;

    FOR idx IN 0 .. length(input_bytes) - 1 LOOP
        current_byte := get_byte(input_bytes, idx);
        IF
            (current_byte BETWEEN 48 AND 57) OR
            (current_byte BETWEEN 65 AND 90) OR
            (current_byte BETWEEN 97 AND 122) OR
            current_byte IN (45, 46, 95, 126)
        THEN
            encoded := encoded || chr(current_byte);
        ELSIF current_byte = 32 THEN
            encoded := encoded || '%20';
        ELSE
            encoded := encoded || '%' || upper(lpad(to_hex(current_byte), 2, '0'));
        END IF;
    END LOOP;

    RETURN encoded;
END;
$$;

CREATE OR REPLACE VIEW tally_case_export AS
SELECT
    ru.folder_id,
    f.folder_name,
    f.folder_path,
    ru.execution_day,
    ds.scenario_key,
    COALESCE(NULLIF(tsi.scenario_title, ''), NULLIF(ds.scenario_title, ''), NULLIF(ru.run_name, ''), 'Ungrouped') AS scenario_title,
    ds.scenario_order,
    ds.cycle_order,
    COALESCE(NULLIF(ru.cycle_id, ''), ru.test_run_id) AS cycle_ref,
    ru.test_run_id,
    ru.cycle_id,
    ru.run_name AS cycle_name,
    ru.cycle_objective,
    tr.test_result_id,
    tr.test_case_key,
    tr.test_case_name,
    tr.current_status_name,
    tr.desired_status_id,
    tr.desired_comment,
    tr.sync_state,
    tr.updated_at,
    tsf.tally_form_id,
    tsf.tally_form_url,
    CONCAT(
        COALESCE(tsf.tally_form_url, ''),
        CASE WHEN POSITION('?' IN COALESCE(tsf.tally_form_url, '')) > 0 THEN '&' ELSE '?' END,
        'folder_id=', url_encode_utf8(COALESCE(ru.folder_id, '')),
        '&scenario_key=', url_encode_utf8(COALESCE(ds.scenario_key, '')),
        '&scenario_title=', url_encode_utf8(COALESCE(tsi.scenario_title, ds.scenario_title, ''))
    ) AS tally_prefilled_url
FROM test_runs ru
JOIN test_results tr ON tr.test_run_id = ru.test_run_id
LEFT JOIN folders f ON f.folder_id = ru.folder_id
LEFT JOIN LATERAL derive_scenario_from_run_name(ru.run_name) ds ON TRUE
LEFT JOIN tally_scenario_index tsi
    ON tsi.folder_id = ru.folder_id
   AND tsi.scenario_key = ds.scenario_key
LEFT JOIN tally_scenario_forms tsf
    ON tsf.folder_id = ru.folder_id
   AND tsf.scenario_key = ds.scenario_key
   AND tsf.is_active = TRUE;

COMMIT;
