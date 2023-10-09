
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_4_v1`
  GROUP BY
    first_seen_date,
    first_reported_country,
    first_reported_isp,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['first_seen_date', 'first_reported_country', 'first_reported_isp', 'adjust_ad_group', 'adjust_campaign', 'adjust_creative', 'adjust_network'] to be unique.)"
    ),
    NULL
  );

#fail
WITH null_checks AS (
  SELECT
    [
      IF(COUNTIF(first_seen_date IS NULL) > 0, "first_seen_date", NULL),
      IF(COUNTIF(adjust_network IS NULL) > 0, "adjust_network", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_4_v1`
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
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_4_v1`
  WHERE
    submission_date = @submission_date
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

#fail
WITH new_profile_count AS (
  SELECT
    SUM(new_profiles)
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_4_v1`
  WHERE
    submission_date = @submission_date
),
new_profile_upstream_count AS (
  SELECT
    COUNT(*)
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_clients_week_4_v1`
  WHERE
    submission_date = @submission_date
)
SELECT
  IF(
    (SELECT * FROM new_profile_count) <> (SELECT * FROM new_profile_upstream_count),
    ERROR(
      CONCAT(
        "New profile count mismatch between this (",
        (SELECT * FROM new_profile_count),
        ") and upstream (",
        (SELECT * FROM new_profile_upstream_count),
        ") tables"
      )
    ),
    NULL
  );

#fail
WITH repeat_user_count AS (
  SELECT
    SUM(repeat_user)
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_4_v1`
  WHERE
    submission_date = @submission_date
),
repeat_user_upstream_count AS (
  SELECT
    COUNTIF(repeat_first_month_user)
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_clients_week_4_v1`
  WHERE
    submission_date = @submission_date
)
SELECT
  IF(
    (SELECT * FROM repeat_user_count) <> (SELECT * FROM repeat_user_upstream_count),
    ERROR(
      CONCAT(
        "New profile count mismatch between this (",
        (SELECT * FROM repeat_user_count),
        ") and upstream (",
        (SELECT * FROM repeat_user_upstream_count),
        ") tables"
      )
    ),
    NULL
  );

#fail
WITH retained_week_4_count AS (
  SELECT
    SUM(retained_week_4)
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_4_v1`
  WHERE
    submission_date = @submission_date
),
retained_week_4_upstream_count AS (
  SELECT
    COUNTIF(retained_week_4)
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_clients_week_4_v1`
  WHERE
    submission_date = @submission_date
)
SELECT
  IF(
    (SELECT * FROM retained_week_4_count) <> (SELECT * FROM retained_week_4_upstream_count),
    ERROR(
      CONCAT(
        "New profile count mismatch between this (",
        (SELECT * FROM retained_week_4_count),
        ") and upstream (",
        (SELECT * FROM retained_week_4_upstream_count),
        ") tables"
      )
    ),
    NULL
  );

#fail
SELECT
  IF(
    DATE_DIFF(submission_date, first_seen_date, DAY) <> 27,
    ERROR(
      "Day difference between submission_date and first_seen_date is not equal to 27 as expected"
    ),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_4_v1`
WHERE
  submission_date = @submission_date;
