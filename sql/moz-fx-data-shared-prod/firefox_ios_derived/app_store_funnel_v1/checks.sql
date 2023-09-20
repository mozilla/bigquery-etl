#fail
{{ is_unique(["`date`", "country"]) }}
-- min_row_count helps us detect if we're seeing delays in the data arriving
-- could also be an indicator of an upstream issue.
#fail
{{ min_row_count(1, "`date` = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)") }}
#fail
WITH _aua_new_profiles AS (
  SELECT
    SUM(new_profiles) AS new_profiles,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.active_users_aggregates`
  WHERE
    submission_date = @submission_date
    AND channel = "release"
),
_new_funnel_new_profiles AS (
  SELECT
    SUM(new_profiles) AS new_funnel_new_profiles,
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    `date` = @submission_date
)
SELECT
  IF(
    (SELECT * FROM _aua_new_profiles) - (SELECT * FROM _new_funnel_new_profiles) <> 0,
    ERROR("There's a 'new_profiles' mismatch between active_users_aggregates and the funnel table"),
    NULL
  );

#warn
WITH base AS (
  SELECT
    SUM(total_downloads) AS total_downloads,
    SUM(first_time_downloads) AS first_time_downloads,
    SUM(redownloads) AS redownloads,
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    `date` = @submission_date
)
SELECT
  IF(
    total_downloads <> (first_time_downloads + redownloads),
    ERROR("The sum of first time downloads and redownloads does not match that of total_downloads"),
    NULL
  )
FROM
  base;

#warn
WITH base AS (
  SELECT
    SUM(new_profiles) AS new_funnel_new_profiles,
    SUM(total_downloads) AS total_downloads,
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    `date` = @submission_date
)
SELECT
  IF(
    new_funnel_new_profiles > total_downloads,
    ERROR("There are more new_profiles than app downloads."),
    NULL
  )
FROM
  base;
