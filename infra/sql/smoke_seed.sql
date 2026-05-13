BEGIN;

INSERT INTO folders(folder_id, folder_name, folder_path)
VALUES ('f-demo-1', 'Demo Folder', '/demo')
ON CONFLICT (folder_id) DO UPDATE
SET folder_name = EXCLUDED.folder_name,
    folder_path = EXCLUDED.folder_path;

INSERT INTO test_runs(test_run_id, folder_id, cycle_id, run_name, execution_day, source_status_name)
VALUES ('tr-demo-1', 'f-demo-1', 'c-demo-1', 'Demo Run', CURRENT_DATE, 'Fail')
ON CONFLICT (test_run_id) DO UPDATE
SET run_name = EXCLUDED.run_name,
    source_status_name = EXCLUDED.source_status_name;

INSERT INTO test_results(
    test_result_id,
    test_run_id,
    test_case_key,
    current_status_name,
    desired_status_name,
    desired_status_id,
    desired_comment,
    sync_state
)
VALUES (
    'res-demo-1',
    'tr-demo-1',
    'QA-T1',
    'Fail',
    'Pass',
    '3',
    'Smoke sync request',
    'pending'
)
ON CONFLICT (test_result_id) DO UPDATE
SET desired_status_name = EXCLUDED.desired_status_name,
    desired_status_id = EXCLUDED.desired_status_id,
    desired_comment = EXCLUDED.desired_comment,
    sync_state = 'pending',
    updated_at = NOW();

INSERT INTO sync_queue(test_result_id, operation_hash, payload_json, status)
VALUES (
    'res-demo-1',
    'op-demo-1',
    '{"desired_status_id":3,"desired_comment":"Smoke sync request"}'::jsonb,
    'queued'
)
ON CONFLICT (test_result_id, operation_hash) DO NOTHING;

COMMIT;
