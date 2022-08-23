CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.marketing_attributable_metrics`
AS
WITH dau_client AS (
  SELECT
    submission_date,
    client_id,
    first_seen_date,
    country,
    COUNTIF(days_since_seen < 1) AS dau,
    COUNTIF(first_seen_date = submission_date) AS new_profiles
  FROM
    `moz-fx-data-shared-prod.telemetry.fenix_clients_last_seen`
  WHERE
    submission_date >= '2021-01-01'
    AND days_since_seen < 1
  GROUP BY
    1,
    2,
    3,
    4
),
search AS (
  SELECT
    submission_date,
    client_id,
    SUM(search_count) AS search_count,
    SUM(ad_click) AS ad_clicks
  FROM
    `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v1`
  WHERE
    submission_date >= '2021-01-01'
    AND normalized_app_name = 'Fenix'
    AND os = 'Android'
  GROUP BY
    1,
    2
),
adjust_client AS (
  SELECT
    client_info.client_id AS client_id,
    ARRAY_AGG(metrics.string.first_session_network)[SAFE_OFFSET(0)] AS adjust_network,
    ARRAY_AGG(metrics.string.first_session_adgroup)[SAFE_OFFSET(0)] AS adjust_adgroup,
    ARRAY_AGG(metrics.string.first_session_campaign)[SAFE_OFFSET(0)] AS adjust_campaign,
    ARRAY_AGG(metrics.string.first_session_creative)[SAFE_OFFSET(0)] AS adjust_creative,
    MIN(DATE(submission_timestamp)) AS first_session_date
  FROM
    `mozdata.fenix.first_session`
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND metrics.string.first_session_network IS NOT NULL
    AND metrics.string.first_session_network <> ''
  GROUP BY
    1
)
SELECT
  submission_date,
  first_seen_date AS cohort_date,
  client_id,
  country,
  adjust_network,
  adjust_adgroup,
  adjust_campaign,
  adjust_creative,
  new_profiles,
  dau,
  coalesce(search_count, 0) AS search_count,
  coalesce(ad_clicks, 0) AS ad_clicks
FROM
  dau_client
LEFT JOIN
  search
USING
  (client_id, submission_date)
JOIN
  adjust_client
USING
  (client_id)
