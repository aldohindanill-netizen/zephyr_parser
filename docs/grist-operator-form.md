# Grist document-per-folder setup

This setup creates one Grist document per Zephyr subfolder on every sync run.

## 1) Document model

For each Zephyr folder, n8n ensures a dedicated Grist document and a table
(`Table1` by default) with exactly six operator-facing columns:

1. `cycle_name`
2. `test_case_name`
3. `objective`
4. `done` (bool)
5. `not_done` (bool)
6. `comment`

`done` and `not_done` are mutually exclusive by workflow rules (enforced in both
`grist_to_postgres_sync` and `postgres_to_grist_status_sync`):

- `done=true` + `not_done=false` -> status id `145`
- `done=false` + `not_done=true` -> status id `146`
- both true/false -> ignored (no writeback)

## 2) Folder-to-document registry

Postgres table `grist_folder_docs` stores stable mapping:

- `folder_id`
- `grist_doc_id`
- `grist_table_id`
- `grist_doc_name`
- `grist_doc_url`

This guarantees each folder keeps the same shareable document URL between runs.

## 3) Workflows

- `workflows/grist_to_postgres_sync.json`
  - discovers folders under configured parent
  - creates missing Grist documents
  - ensures table/columns
  - upserts rows from Postgres into Grist
  - reads operator decisions back from Grist and updates `test_results`
- `workflows/postgres_to_grist_status_sync.json`
  - re-syncs `done/not_done/comment` from Postgres to Grist

## 4) Runtime variables

Set in `infra/.env.nocodb-n8n`:

- `GRIST_API_URL`
- `GRIST_DOCS_BASE_URL`
- `GRIST_PARENT_WORKSPACE_ID` (optional)
- `GRIST_ZEPHYR_PARENT_FOLDER_ID` (required for folder scope)
- `GRIST_API_TOKEN`
- `GRIST_TABLE_ID` (default `Table1`)
- `GRIST_MAX_RECORDS`
- `GRIST_STATUS_LOOKBACK_HOURS`
- `GRIST_DONE_STATUS_ID` (default `145`)
- `GRIST_NOT_DONE_STATUS_ID` (default `146`)

## 5) Sharing

Each row in `grist_folder_docs.grist_doc_url` is a document link that can be
shared directly with operators assigned to that folder.

## 6) VPN-offline validation

Without Zephyr network access (for example, outside corporate VPN), you can
still validate the integration up to queueing:

- edit `D/E/F` (`done/not_done/comment`) in one folder document table
- wait one `grist_to_postgres_sync` tick
- verify in Postgres:
  - `test_results.desired_status_id` and `test_results.desired_comment` updated
  - `sync_queue` contains a new `queued` row for that `test_result_id`

This confirms `Grist -> Postgres -> sync_queue` is healthy.

You can also run SQL-only verification via
`infra/sql/query_form_to_queue_offline_check.sql` to inspect:

- final `desired_status_id` (`145`/`146`) in `test_results`
- queued payload in `sync_queue`
- latest writeback attempts in `sync_audit` (can be empty while offline)

## 7) Post-VPN final check

After VPN access is restored, complete the final hop:

- run or wait `zephyr_writeback_15m`
- verify `sync_queue` row transitions from `queued` to processed state
- verify `sync_audit` has a success record
- verify status/comment changed in Zephyr

## 8) Helper scripts

Run from repo root in PowerShell:

- Offline check (no VPN required):
  - `.\scripts\offline-sync-check.ps1 -TestResultId 112522`
- Post-VPN smoke (waits and verifies writeback/audit):
  - `.\scripts\post-vpn-smoke.ps1 -TestResultId 112522 -WaitSeconds 120`

Both scripts accept optional env/compose overrides:

- `-EnvFile infra/.env.nocodb-n8n`
- `-ComposeFile infra/docker-compose.nocodb-n8n.yml`
