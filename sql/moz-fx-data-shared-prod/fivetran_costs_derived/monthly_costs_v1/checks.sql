
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.monthly_costs_v1`
  GROUP BY
    destination_id,
    measured_month
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['destination_id', 'measured_month'] to be unique.)"
    ),
    NULL
  );

#fail
WITH null_checks AS (
  SELECT
    [
      IF(COUNTIF(destination_id IS NULL) > 0, "destination_id", NULL),
      IF(COUNTIF(measured_month IS NULL) > 0, "measured_month", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.monthly_costs_v1`
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
    `moz-fx-data-shared-prod.fivetran_costs_derived.monthly_costs_v1`
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
