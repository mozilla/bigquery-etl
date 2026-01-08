-- Query for search_derived.legacy_ad_clicks_by_state_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
WITH ad_click_clients AS (
  SELECT
    submission_date,
    client_id,
    `moz-fx-data-shared-prod`.udf.normalize_search_engine(engine) AS normalized_engine,
    ad_click
  FROM
    `moz-fx-data-shared-prod.search_derived.search_clients_daily_v8`
  WHERE
    submission_date = @submission_date
    AND country = 'US'
    AND `moz-fx-data-shared-prod`.udf.normalize_search_engine(engine) IN (
      'Bing',
      'DuckDuckGo',
      'Google'
    )
),
ad_click_states AS (
  SELECT
    submission_date,
    client_id,
    geo_subdivision1 AS `state`
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_joined_v1`
  WHERE
    submission_date = @submission_date
    AND country = 'US'
    AND client_id IN (SELECT client_id FROM ad_click_clients)
),
join_states_and_clients AS (
  SELECT
    ad_click_clients.submission_date,
    DATE_TRUNC(ad_click_clients.submission_date, month) AS `month`,
    ad_click_clients.client_id,
    ad_click_clients.normalized_engine,
    ad_click_clients.ad_click,
    ad_click_states.state
  FROM
    ad_click_clients
  LEFT JOIN
    ad_click_states
    ON ad_click_clients.submission_date = ad_click_states.submission_date
    AND ad_click_clients.client_id = ad_click_states.client_id
),
daily_table AS (
  SELECT
    submission_date,
    `month`,
    normalized_engine,
    `state`,
    SUM(ad_click) AS daily_ad_clicks
  FROM
    join_states_and_clients
  GROUP BY
    submission_date,
    `month`,
    normalized_engine,
    `state`
),
final AS (
  SELECT
    *
  FROM
    daily_table
)
SELECT
  *
FROM
  final
