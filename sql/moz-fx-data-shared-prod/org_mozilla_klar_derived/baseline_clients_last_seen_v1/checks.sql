
-- Generated via bigquery_etl.glean_usage
#warn
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_derived.baseline_clients_last_seen_v1`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['client_id'] to be unique.)"
    ),
    NULL
  );

#warn
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_derived.baseline_clients_last_seen_v1`
  WHERE
    submission_date = @submission_date
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

# warn
WITH null_checks AS (
  SELECT
    [
      IF(COUNTIF(submission_date IS NULL) > 0, "submission_date", NULL),
      IF(COUNTIF(client_id IS NULL) > 0, "client_id", NULL),
      IF(COUNTIF(sample_id IS NULL) > 0, "sample_id", NULL),
      IF(COUNTIF(first_seen_date IS NULL) > 0, "first_seen_date", NULL),
      IF(COUNTIF(days_seen_bits IS NULL) > 0, "days_seen_bits", NULL),
      IF(COUNTIF(days_created_profile_bits IS NULL) > 0, "days_created_profile_bits", NULL),
      IF(COUNTIF(days_seen_session_start_bits IS NULL) > 0, "days_seen_session_start_bits", NULL),
      IF(COUNTIF(days_seen_session_end_bits IS NULL) > 0, "days_seen_session_end_bits", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_derived.baseline_clients_last_seen_v1`
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

#warn
SELECT
  IF(
    COUNTIF(normalized_channel NOT IN ("nightly", "aurora", "release", "Other", "beta", "esr")) > 0,
    ERROR("Unexpected values for field normalized_channel detected."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_klar_derived.baseline_clients_last_seen_v1`
WHERE
  submission_date = @submission_date;

#warn
SELECT
  IF(
    COUNTIF(NOT REGEXP_CONTAINS(country, r"^[A-Z]{2}$")) > 0,
    ERROR(
      "Some values inside the `country` column do not match the expected pattern: `^[A-Z]{2}$`"
    ),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_klar_derived.baseline_clients_last_seen_v1`
WHERE
  submission_date = @submission_date;

#warn
SELECT
  IF(
    COUNTIF(LENGTH(client_id) <> 36) > 0,
    ERROR("Column: `client_id` has values of unexpected length."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_klar_derived.baseline_clients_last_seen_v1`
WHERE
  submission_date = @submission_date;
