#fail
{{ is_unique(["submission_date", "country"], "country IS NOT NULL") }}
-- min_row_count helps us detect if we're seeing delays in the data arriving
-- could also be an indicator of an upstream issue.
#fail
{{ min_row_count(1, "submission_date = @submission_date") }}
#fail
WITH fx_ios_count AS (
  SELECT
    COUNT(*)
  FROM
    `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND channel = "release"
),
new_profiles_count AS (
  SELECT
    SUM(new_profiles)
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
)
SELECT
  IF(
    (SELECT * FROM fx_ios_count) - (SELECT * FROM new_profiles_count) <> 0,
    ERROR("There's a 'new_profiles' mismatch between firefox_ios_clients and this funnel table"),
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
    submission_date = @submission_date
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
    submission_date = @submission_date
)
SELECT
  IF(
    new_funnel_new_profiles > total_downloads,
    ERROR("There are more new_profiles than app downloads."),
    NULL
  )
FROM
  base;

#fail
SELECT
  IF(
    DATE_DIFF(submission_date, first_seen_date, DAY) <> 7,
    ERROR("Day difference between submission_date and `date` is not equal to 7 as expected"),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.app_store_funnel_v1`
WHERE
  submission_date = @submission_date;

-- TODO: for this query it'd be useful to compare sum variance between each day to improve our confidence the data was complete at the execution time.
