SELECT LEFT("data"::text, 8000) AS data_snippet
FROM execution_data
WHERE "executionId" = 15;
