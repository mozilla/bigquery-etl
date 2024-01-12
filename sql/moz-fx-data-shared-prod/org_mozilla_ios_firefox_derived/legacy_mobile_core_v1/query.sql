CREATE TEMP FUNCTION rename_search(value STRING) AS (
    -- in glean, this is implemented as search-engine-name.source
  CONCAT(SPLIT(value, ".")[OFFSET(1)], ".", SPLIT(value, ".")[OFFSET(0)])
);

WITH extracted AS (
  SELECT
    document_id,
    submission_timestamp,
    client_id,
    DATE(submission_timestamp) AS submission_date,
    searches,
    default_search,
    default_mail_client,
    default_new_tab_experience,
    open_tab_count
  FROM
    `moz-fx-data-shared-prod.telemetry.core`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_os = 'iOS'
    AND normalized_app_name = 'Fennec'
),
searches AS (
  SELECT
    submission_date,
    client_id,
    rename_search(s.key) AS key,
    SUM(s.value) AS value
  FROM
    extracted,
    UNNEST(searches) s
  GROUP BY
    1,
    2,
    3
),
searches_grouped AS (
  SELECT
    submission_date,
    client_id,
    ARRAY_AGG(STRUCT(key, value)) AS labeled_counter_search_counts
  FROM
    searches
  GROUP BY
    1,
    2
),
rest_grouped AS (
  SELECT
    submission_date,
    client_id,
    ANY_VALUE(document_id) AS document_id,
    MIN(submission_timestamp) AS submission_timestamp,
    mozfun.stats.mode_last(ARRAY_AGG(default_search)) AS string_search_default_engine,
    mozfun.stats.mode_last(ARRAY_AGG(default_mail_client)) AS string_preferences_mail_client,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_new_tab_experience)
    ) AS string_preferences_new_tab_experience,
    -- NOTE: that we cannot recover the average count because we are summing this up into one ping. An
    -- average might be more desirable, but it doesn't quite match the metric name.
    SUM(open_tab_count) AS counter_tabs_cumulative_count
  FROM
    extracted
  GROUP BY
    1,
    2
)
SELECT
  *
FROM
  searches_grouped
JOIN
  rest_grouped
  USING (submission_date, client_id)
