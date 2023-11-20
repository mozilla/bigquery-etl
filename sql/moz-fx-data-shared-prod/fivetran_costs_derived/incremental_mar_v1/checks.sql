
#fail
WITH null_checks AS (
  SELECT
    [
      IF(COUNTIF(measured_date IS NULL) > 0, "measured_date", NULL),
      IF(COUNTIF(measured_month IS NULL) > 0, "measured_month", NULL),
      IF(COUNTIF(destination_id IS NULL) > 0, "destination_id", NULL),
      IF(COUNTIF(connector IS NULL) > 0, "connector", NULL),
      IF(COUNTIF(table_name IS NULL) > 0, "table_name", NULL),
      IF(COUNTIF(billing_type IS NULL) > 0, "billing_type", NULL),
      IF(COUNTIF(active_rows IS NULL) > 0, "active_rows", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.incremental_mar_v1`
),
non_null_checks AS (
  SELECT
    ARRAY_AGG(u IGNORE NULLS) AS checks
  FROM
    null_checks,
    UNNEST(checks) AS u
)
SELECT
  IF(
    (SELECT ARRAY_LENGTH(checks) FROM non_null_checks) > 0,
    ERROR(
      CONCAT(
        "Columns with NULL values: ",
        (SELECT ARRAY_TO_STRING(checks, ", ") FROM non_null_checks)
      )
    ),
    NULL
  );

#fail
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.incremental_mar_v1`
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Min Row Count Error: ",
        (SELECT total_rows FROM min_row_count),
        " rows found, expected more than 1 rows"
      )
    ),
    NULL
  );
