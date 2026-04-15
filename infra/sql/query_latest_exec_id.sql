SELECT id, status
FROM execution_entity
WHERE "workflowId" = 'zephyr_writeback_15m'
ORDER BY id DESC
LIMIT 1;
