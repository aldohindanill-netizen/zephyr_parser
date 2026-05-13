WITH ranked AS (
    SELECT
        folder_id,
        scenario_key,
        scenario_title,
        tally_form_id,
        tally_form_name,
        tally_form_url,
        is_active,
        last_synced_at,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY folder_id, scenario_key
            ORDER BY is_active DESC, updated_at DESC, last_synced_at DESC NULLS LAST, tally_form_id DESC
        ) AS keep_rank,
        COUNT(*) OVER (PARTITION BY folder_id, scenario_key) AS duplicate_count
    FROM tally_scenario_forms
)
SELECT
    folder_id,
    scenario_key,
    scenario_title,
    tally_form_id,
    tally_form_name,
    tally_form_url,
    is_active,
    updated_at,
    duplicate_count,
    CASE WHEN keep_rank = 1 THEN 'keep' ELSE 'review_or_deactivate' END AS recommended_action
FROM ranked
WHERE duplicate_count > 1
ORDER BY folder_id, scenario_key, keep_rank, updated_at DESC;
