# n8n Postgres credential for batch workflow

`workflows/zephyr_writeback_15m.json` now reads/writes real queue tables via
`n8n-nodes-base.postgres` nodes.

It expects a credential named:

- `PostgresZephyrOps`

## Create credential in n8n UI

1. Open `http://localhost:5678`.
2. Go to **Credentials** -> **Create Credential** -> **Postgres**.
3. Set values:
   - Host: `postgres`
   - Port: `5432`
   - Database: `zephyr_ops`
   - User: `zephyr`
   - Password: value of `POSTGRES_PASSWORD` from `infra/.env.nocodb-n8n`
   - SSL: disabled
4. Save with exact name: `PostgresZephyrOps`.

## Activate batch workflow

1. Re-import `workflows/zephyr_writeback_15m.json` if needed.
2. Open workflow `zephyr_writeback_15m`.
3. Ensure both Postgres nodes are bound to `PostgresZephyrOps`.
4. Activate workflow.

## Runtime behavior

- Reads ready rows from `sync_queue`.
- Calls Zephyr write-back endpoint per row.
- Writes result to:
  - `sync_queue` (`status`, `attempt_count`, `next_retry_at`, `last_error`)
  - `test_results` (`sync_state`, `sync_error`, `last_synced_*`)
  - `sync_audit` (request/response log)
