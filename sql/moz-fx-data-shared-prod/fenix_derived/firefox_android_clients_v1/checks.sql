
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
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
    [
      IF(COUNTIF(client_id IS NULL) > 0, "client_id", NULL),
      IF(COUNTIF(sample_id IS NULL) > 0, "sample_id", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
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
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
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

#warn
WITH base AS (
  SELECT
    COUNTIF(activated)
  FROM
    `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
  WHERE
    first_seen_date = @submission_date
),
upstream AS (
  SELECT
    COUNTIF(activated = 1)
  FROM
    `moz-fx-data-shared-prod.fenix_derived.new_profile_activation_v1`
  WHERE
    first_seen_date = @submission_date
    AND submission_date = DATE_SUB(@submission_date, INTERVAL 6 DAY)
)
SELECT
  IF(
    (SELECT * FROM base) <> (SELECT * FROM upstream),
    ERROR(
      CONCAT(
        "Number of activations does not match up that of the upstream table. Upstream count: ",
        (SELECT * FROM upstream),
        ", base count: ",
        (SELECT * FROM base)
      )
    ),
    NULL
  );
