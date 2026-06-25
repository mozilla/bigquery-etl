-- Intentionally invalid SQL, used to exercise `bqetl backfill validate --dry-run`.
-- The trailing comma and missing FROM make this fail the dry run.
SELECT
  run_date,
FROM
WHERE
  run_date = @submission_date
