
#fail
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
  WHERE
    submission_date = @submission_date
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
WITH null_checks AS (
  SELECT
    [
      IF(COUNTIF(submission_date IS NULL) > 0, "submission_date", NULL),
      IF(COUNTIF(client_id IS NULL) > 0, "client_id", NULL),
      IF(COUNTIF(first_seen_date IS NULL) > 0, "first_seen_date", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
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
WITH rows_per_partition AS (
  SELECT
    PARSE_DATE("%Y%m%d", partition_id) AS table_partition,
    total_rows
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    table_name = "unified_metrics_v1"
    AND partition_id != "__NULL__"
    AND PARSE_DATE("%Y%m%d", partition_id)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 7 + 1 DAY)
    AND DATE(@submission_date)
),
row_counts_current_and_historic AS (
  SELECT
    SUM(IF(table_partition = @submission_date, total_rows, NULL)) AS current_partition_row_count,
    AVG(
      IF(table_partition < @submission_date, total_rows, NULL)
    ) AS historic_partition_avg_row_count,
  FROM
    rows_per_partition
),
row_count_boundaries AS (
  SELECT
    CAST(current_partition_row_count AS INT64) AS current_partition_row_count,
    CAST(historic_partition_avg_row_count * (1 - 5 / 100) AS INT64) AS lower_bound,
    CAST(historic_partition_avg_row_count * (1 + 5 / 100) AS INT64) AS upper_bound
  FROM
    row_counts_current_and_historic
)
SELECT
  IF(
    current_partition_row_count NOT
    BETWEEN lower_bound
    AND upper_bound,
    ERROR(
      CONCAT(
        "The row count for partition ",
        @submission_date,
        " is outside of the expected boundaries. ",
        "Row count for the current_partition: ",
        current_partition_row_count,
        ". Expected range: ",
        lower_bound,
        " - ",
        upper_bound
      )
    ),
    NULL
  )
FROM
  row_count_boundaries;

#fail
SELECT
  IF(
    COUNTIF(LENGTH(client_id) <> 36) > 0,
    ERROR("Column: `client_id` has values of unexpected length."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
WHERE
  submission_date = @submission_date;

#warn
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
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
WITH null_checks AS (
  SELECT
    [
      IF(COUNTIF(activity_segment IS NULL) > 0, "activity_segment", NULL),
      IF(COUNTIF(normalized_app_name IS NULL) > 0, "normalized_app_name", NULL),
      IF(COUNTIF(normalized_channel IS NULL) > 0, "normalized_channel", NULL),
      IF(COUNTIF(country IS NULL) > 0, "country", NULL),
      IF(COUNTIF(days_seen_bits IS NULL) > 0, "days_seen_bits", NULL),
      IF(COUNTIF(days_since_first_seen IS NULL) > 0, "days_since_first_seen", NULL),
      IF(COUNTIF(days_since_seen IS NULL) > 0, "days_since_seen", NULL),
      IF(COUNTIF(is_new_profile IS NULL) > 0, "is_new_profile", NULL),
      IF(COUNTIF(normalized_os IS NULL) > 0, "normalized_os", NULL),
      IF(COUNTIF(normalized_os_version IS NULL) > 0, "normalized_os_version", NULL),
      IF(COUNTIF(app_version IS NULL) > 0, "app_version", NULL),
      IF(COUNTIF(os_version_major IS NULL) > 0, "os_version_major", NULL),
      IF(COUNTIF(os_version_minor IS NULL) > 0, "os_version_minor", NULL),
      IF(COUNTIF(os_version_patch IS NULL) > 0, "os_version_patch", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
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
    COUNTIF(LENGTH(country) <> 2) > 0,
    ERROR("Column: `country` has values of unexpected length."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
WHERE
  submission_date = @submission_date;

#warn
SELECT
  IF(
    COUNTIF(
      normalized_app_name NOT IN (
        "Firefox Desktop",
        "Firefox iOS",
        "Firefox iOS BrowserStack",
        "Focus iOS",
        "Focus iOS BrowserStack",
        "Fenix",
        "Fenix BrowserStack",
        "Focus Android",
        "Focus Android Glean",
        "Focus Android Glean BrowserStack",
        "Klar iOS"
      )
    ) > 0,
    ERROR("Unexpected values for field normalized_app_name detected."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
WHERE
  submission_date = @submission_date;

#warn
SELECT
  IF(
    COUNTIF(
      activity_segment NOT IN (
        "casual_user",
        "regular_user",
        "infrequent_user",
        "other",
        "core_user"
      )
    ) > 0,
    ERROR("Unexpected values for field activity_segment detected."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
WHERE
  submission_date = @submission_date;

#warn
SELECT
  IF(
    COUNTIF(normalized_channel NOT IN ("nightly", "aurora", "release", "Other", "beta", "esr")) > 0,
    ERROR("Unexpected values for field normalized_channel detected."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
WHERE
  submission_date = @submission_date;

#warn
SELECT
  IF(
    COUNTIF(NOT REGEXP_CONTAINS(CAST(country AS STRING), r"^[A-Z]{2}|\?\?$")) > 0,
    ERROR("Unexpected values for field normalized_channel detected."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
WHERE
  submission_date = @submission_date;
