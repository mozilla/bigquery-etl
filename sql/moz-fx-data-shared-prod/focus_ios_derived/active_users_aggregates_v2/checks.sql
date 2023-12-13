
#warn
WITH dau_sum AS (
  SELECT
    SUM(dau),
  FROM
    `moz-fx-data-shared-prod.focus_ios_derived.active_users_aggregates_v2`
  WHERE
    submission_date = @submission_date
),
distinct_client_count_base AS (
  SELECT
    COUNT(DISTINCT client_info.client_id) AS distinct_client_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
distinct_client_count AS (
  SELECT
    SUM(distinct_client_count)
  FROM
    distinct_client_count_base
)
SELECT
  IF(
    (SELECT * FROM dau_sum) <> (SELECT * FROM distinct_client_count),
    ERROR("DAU mismatch between aggregates table and live table"),
    NULL
  );
