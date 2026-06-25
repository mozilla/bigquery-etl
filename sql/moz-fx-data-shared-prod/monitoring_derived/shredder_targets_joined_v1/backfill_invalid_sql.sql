-- Minimal valid custom query, used to exercise `bqetl backfill validate --dry-run-custom-query`.
-- Returns a subset of columns from schema.yaml with matching types.
SELECT
  @submission_date AS run_date,
  CAST(NULL AS STRING) AS project_id,
  CAST(NULL AS BOOL) AS matching_sources
