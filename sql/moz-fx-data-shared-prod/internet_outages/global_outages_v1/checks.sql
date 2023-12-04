
#fail
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.internet_outages.global_outages_v1`
  WHERE
    DATE(datetime) = @submission_date
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1000) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Min Row Count Error: ",
        (SELECT total_rows FROM min_row_count),
        " rows found, expected more than 1000 rows"
      )
    ),
    NULL
  );

#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.internet_outages.global_outages_v1`
  WHERE
    DATE(`datetime`) = @submission_date
  GROUP BY
    datetime,
    city,
    country
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['datetime', 'city', 'country'] to be unique.)"
    ),
    NULL
  );

#fail
WITH null_checks AS (
  SELECT
    [
      IF(COUNTIF(datetime IS NULL) > 0, "datetime", NULL),
      IF(COUNTIF(city IS NULL) > 0, "city", NULL),
      IF(COUNTIF(country IS NULL) > 0, "country", NULL),
      IF(COUNTIF(proportion_undefined IS NULL) > 0, "proportion_undefined", NULL),
      IF(COUNTIF(proportion_timeout IS NULL) > 0, "proportion_timeout", NULL),
      IF(COUNTIF(proportion_abort IS NULL) > 0, "proportion_abort", NULL),
      IF(COUNTIF(proportion_unreachable IS NULL) > 0, "proportion_unreachable", NULL),
      IF(COUNTIF(proportion_terminated IS NULL) > 0, "proportion_terminated", NULL),
      IF(COUNTIF(proportion_channel_open IS NULL) > 0, "proportion_channel_open", NULL),
      IF(COUNTIF(avg_dns_success_time IS NULL) > 0, "avg_dns_success_time", NULL),
      IF(COUNTIF(missing_dns_success IS NULL) > 0, "missing_dns_success", NULL),
      IF(COUNTIF(avg_dns_failure_time IS NULL) > 0, "avg_dns_failure_time", NULL),
      IF(COUNTIF(missing_dns_failure IS NULL) > 0, "missing_dns_failure", NULL),
      IF(COUNTIF(count_dns_failure IS NULL) > 0, "count_dns_failure", NULL),
      IF(COUNTIF(ssl_error_prop IS NULL) > 0, "ssl_error_prop", NULL),
      IF(COUNTIF(avg_tls_handshake_time IS NULL) > 0, "avg_tls_handshake_time", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.internet_outages.global_outages_v1`
  WHERE
    DATE(`datetime`) = @submission_date
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
SELECT
  IF(
    COUNTIF(LENGTH(country) <> 2) > 0,
    ERROR(
      "Some values in this field do not adhere to the ISO 3166-1 specification (2 character country code)."
    ),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.internet_outages.global_outages_v1`
WHERE
  DATE(`datetime`) = @submission_date;
