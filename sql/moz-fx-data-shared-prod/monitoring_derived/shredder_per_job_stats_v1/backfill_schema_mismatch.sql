-- Valid SQL that returns a column absent from schema.yaml, used to exercise
-- `bqetl backfill validate --dry-run`. The dry run succeeds but the schema check
-- fails because `unexpected_column` is not part of the table's schema.
SELECT
  @submission_date AS end_time,
  1 AS unexpected_column
