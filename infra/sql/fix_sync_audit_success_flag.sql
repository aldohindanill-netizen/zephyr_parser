-- Normalize sync_audit.success using HTTP status code.
-- Use when historical rows are inconsistent (for example old runs marked success on 401).
-- Safe to run repeatedly.

BEGIN;

UPDATE sync_audit
SET success = CASE
  WHEN response_status >= 200 AND response_status < 300 THEN TRUE
  ELSE FALSE
END
WHERE success IS DISTINCT FROM CASE
  WHEN response_status >= 200 AND response_status < 300 THEN TRUE
  ELSE FALSE
END;

COMMIT;
