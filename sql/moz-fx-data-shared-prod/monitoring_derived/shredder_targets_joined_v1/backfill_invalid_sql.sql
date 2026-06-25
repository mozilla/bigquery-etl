-- Syntactically valid SQL that still fails the dry run, used to exercise
-- `bqetl backfill validate --dry-run`. The referenced table does not exist, so
-- BigQuery rejects it during the dry run (not a parse error).
SELECT
  run_date
FROM
  `moz-fx-data-shared-prod.monitoring_derived.this_table_does_not_exist_v1`
WHERE
  run_date = @submission_date
