{% if is_init() %}
INSERT INTO
 `{project_id}.{dataset_id}.{table_id}`
{% endif %}
CREATE TEMP FUNCTION sum_map_values(map ARRAY<STRUCT<key STRING, value INT64>>)
RETURNS INT64 AS (
  (SELECT SUM(value) FROM UNNEST(map))
);

WITH client_days AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    sample_id,
    client_info.client_id,
    LOGICAL_OR(
      mozfun.norm.extract_version(client_info.app_display_version, 'major') >= 107
    ) AS has_search_data,
    SUM(sum_map_values(metrics.labeled_counter.browser_search_in_content)) AS searches,
    SUM(sum_map_values(metrics.labeled_counter.browser_search_with_ads)) AS searches_with_ads,
    SUM(sum_map_values(metrics.labeled_counter.browser_search_ad_clicks)) AS ad_clicks,
  FROM
    `moz-fx-data-shared-prod`.fenix.baseline
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= "2021-08-01"
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    sample_id,
    client_id
),
searches AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    LOGICAL_OR(mozfun.norm.extract_version(app_version, 'major') < 107) AS has_search_data,
    SUM(search_count) AS searches,
    SUM(search_with_ads) AS searches_with_ads,
    SUM(ad_click) AS ad_clicks
  FROM
    `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v1`
  WHERE
    {% if is_init() %}
      submission_date >= "2021-08-01"
    {% else %}
      submission_date = @submission_date
    {% endif %}
    AND normalized_app_name = 'Fenix'
    AND os = 'Android'
  GROUP BY
    submission_date,
    client_id,
    sample_id
),
new_activations AS (
  SELECT
    client_id,
    sample_id,
    submission_date,
    activated > 0 AS activated,
  FROM
    `moz-fx-data-shared-prod`.fenix.new_profile_activation
  WHERE
    {% if is_init() %}
      submission_date >= "2021-08-01"
    {% else %}
      submission_date = @submission_date
    {% endif %}
)
SELECT
  submission_date,
  sample_id,
  client_id,
  -- Metrics
  IF(COALESCE(new_activations.activated, FALSE), 1, 0) AS activations_count,
  IF(COALESCE(client_days.client_id IS NOT NULL, FALSE), 1, 0) AS active_day_count,
  CASE
    WHEN client_days.has_search_data
      THEN client_days.searches
    WHEN metrics_searches.has_search_data
      THEN metrics_searches.searches
    ELSE 0
  END AS searches,
  CASE
    WHEN client_days.has_search_data
      THEN client_days.searches_with_ads
    WHEN metrics_searches.has_search_data
      THEN metrics_searches.searches_with_ads
    ELSE 0
  END AS searches_with_ads,
  CASE
    WHEN client_days.has_search_data
      THEN client_days.ad_clicks
    WHEN metrics_searches.has_search_data
      THEN metrics_searches.ad_clicks
    ELSE 0
  END AS ad_clicks,
  -- Metadata
  STRUCT(
    client_days.client_id IS NOT NULL AS sent_baseline_ping,
    metrics_searches.client_id IS NOT NULL AS sent_metrics_search_data,
    new_activations.client_id IS NOT NULL AS activated_on_this_day
  ) AS metadata,
FROM
  client_days
FULL OUTER JOIN
  searches AS metrics_searches
USING
  (client_id, sample_id, submission_date)
FULL OUTER JOIN
  new_activations
USING
  (client_id, sample_id, submission_date)
