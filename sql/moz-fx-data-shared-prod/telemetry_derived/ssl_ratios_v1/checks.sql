
#fail
WITH null_checks AS (
  SELECT
    [
      IF(COUNTIF(submission_date IS NULL) > 0, "submission_date", NULL),
      IF(COUNTIF(os IS NULL) > 0, "os", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.ssl_ratios_v1`
  WHERE
    submission_date = @submission_date
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
WITH min_rows AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.ssl_ratios_v1`
  WHERE
    submission_date = @submission_date
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1) FROM min_rows) > 0,
    ERROR(
      CONCAT("Less than ", (SELECT total_rows FROM min_rows), " rows found (expected more than 1)")
    ),
    NULL
  );

#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.ssl_ratios_v1`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    os,
    country
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['submission_date', 'os', 'country'] to be unique.)"
    ),
    NULL
  );

#fail
WITH ranges AS (
  SELECT
    [
      IF(COUNTIF(non_ssl_loads < 0) > 0, "non_ssl_loads", NULL),
      IF(COUNTIF(ssl_loads < 0) > 0, "ssl_loads", NULL),
      IF(COUNTIF(reporting_ratio < 0) > 0, "reporting_ratio", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.ssl_ratios_v1`
  WHERE
    submission_date = @submission_date
),
range_checks AS (
  SELECT
    ARRAY_AGG(u IGNORE NULLS) AS checks
  FROM
    ranges,
    UNNEST(checks) AS u
)
SELECT
  IF(
    (SELECT ARRAY_LENGTH(checks) FROM range_checks) > 0,
    ERROR(
      CONCAT(
        "Columns with values not within defined range [0, None]: ",
        (SELECT ARRAY_TO_STRING(checks, ", ") FROM range_checks)
      )
    ),
    NULL
  );
