
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.firefox_ios_clients_v1`
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

#fail
WITH null_checks AS (
  SELECT
    [IF(COUNTIF(client_id IS NULL) > 0, "client_id", NULL)] AS checks
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.firefox_ios_clients_v1`
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
    `moz-fx-data-shared-prod.firefox_ios_derived.firefox_ios_clients_v1`
  WHERE
    first_seen_date = @submission_date
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Less than ",
        (SELECT total_rows FROM min_row_count),
        " rows found (expected more than 1)"
      )
    ),
    NULL
  );

#warn
SELECT
  IF(
    (COUNTIF(is_suspicious_device_client) / COUNT(*)) * 100 > 5,
    ERROR("The % of suspicious device clients exceeds 5%"),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.firefox_ios_clients_v1`
WHERE
  first_seen_date = @submission_date;
