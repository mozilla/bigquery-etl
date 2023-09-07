WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.daily_connector_costs_v1`
  GROUP BY
    destination,
    measured_date,
    connector,
    billing_type
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['destination', 'measured_date', 'connector', 'billing_type'] to be unique.)"
    ),
    NULL
  );

WITH null_checks AS (
  SELECT
    [
      IF(COUNTIF(destination IS NULL) > 0, "destination", NULL),
      IF(COUNTIF(measured_date IS NULL) > 0, "measured_date", NULL),
      IF(COUNTIF(connector IS NULL) > 0, "connector", NULL),
      IF(COUNTIF(billing_type IS NULL) > 0, "billing_type", NULL),
      IF(COUNTIF(active_rows IS NULL) > 0, "active_rows", NULL),
      IF(COUNTIF(cost_in_usd IS NULL) > 0, "cost_in_usd", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.daily_connector_costs_v1`
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

WITH min_rows AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.daily_connector_costs_v1`
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1) FROM min_rows) > 0,
    ERROR(
      CONCAT("Less than ", (SELECT total_rows FROM min_rows), " rows found (expected more than 1)")
    ),
    NULL
  );
