-- Minimal valid custom query, used to exercise `bqetl backfill validate --dry-run-custom-query`.
-- Returns a subset of columns from schema.yaml with matching types.
SELECT
  CAST(@submission_date AS DATE) AS partition_date,
  CAST(NULL AS STRING) AS task_id,
  CAST(NULL AS TIMESTAMP) AS end_time
