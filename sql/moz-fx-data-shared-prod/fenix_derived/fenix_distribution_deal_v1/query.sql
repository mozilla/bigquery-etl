  --For Data Eng: Backfill 1 year
WITH fenix_clients_daily AS (
  SELECT
    client_id,
    submission_date,
    country,
    first_seen_date,
  FROM
    `moz-fx-data-shared-prod.fenix.active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
    AND country IN ('ES', 'AT', 'IT', 'DE', 'FR', 'MX', 'CO')
  GROUP BY
    1,
    2,
    3,
    4
),
fenix_metrics_daily AS (
  SELECT
    client_info.client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    CASE
    --deal_id yet to be implemented by Mobile Eng. Uncomment the following line when the data is ready.
    --WHEN metrics.string.first_session_deal_id = "vivo-cpi-2024" THEN "Vivo CPI Deal 2024"
      WHEN LOWER(client_info.device_manufacturer) LIKE "%vivo%"
        THEN "Vivo Organic"
      ELSE "Not Vivo"
    END AS vivo_status,
    CAST(
      MAX(COALESCE(metrics.boolean.metrics_default_browser, FALSE)) AS bool
    ) AS is_default_browser,
  FROM
    `moz-fx-data-shared-prod.fenix.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id,
    submission_date,
    vivo_status
),
search_clients_daily AS (
    --If Search Eng team implements search codes specific for this project, use those instead.
  SELECT
    client_id,
    submission_date,
    SUM(CASE WHEN normalized_engine = "Google" THEN tagged_sap ELSE 0 END) AS google_tagged_sap,
    SUM(CASE WHEN normalized_engine = "Google" THEN ad_click ELSE 0 END) AS google_ad_click,
    SUM(
      CASE
        WHEN normalized_engine = "Google"
          THEN search_with_ads
        ELSE 0
      END
    ) AS google_search_with_ads,
    SUM(CASE WHEN normalized_engine = "Bing" THEN tagged_sap ELSE 0 END) AS bing_tagged_sap,
    SUM(CASE WHEN normalized_engine = "Bing" THEN ad_click ELSE 0 END) AS bing_ad_click,
    SUM(
      CASE
        WHEN normalized_engine = "Bing"
          THEN search_with_ads
        ELSE 0
      END
    ) AS bing_search_with_ads,
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_engines_sources_daily`
  WHERE
    app_name = "Fenix"
    AND submission_date = @submission_date
  GROUP BY
    client_id,
    submission_date
),
retention_clients_daily AS (
  SELECT
    client_id,
    metric_date AS submission_date,
    --As a metric, retention is always on a 4-week lag.
    retained_week_4 AS retained_4_weeks_from_submission_date,
  FROM
    mozdata.fenix.retention_clients
  WHERE
    metric_date = DATE_SUB(DATE(@submission_date), INTERVAL 28 day)
    AND submission_date = @submission_date  --28 days after metric date
),
sponsored_tile_daily AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    COUNTIF(event_category = "top_sites" AND event_name = "contile_click") AS sponsored_tile_clicks
  FROM
    `moz-fx-data-shared-prod.fenix.events_unnested`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    client_id
)
SELECT
  f.*,
  COALESCE(m.vivo_status, "Not Vivo") AS vivo_status,
  COALESCE(m.is_default_browser, FALSE) AS is_default_browser,
  COALESCE(s.google_tagged_sap, 0) AS google_tagged_sap,
  COALESCE(s.google_search_with_ads, 0) AS google_search_with_ads,
  COALESCE(s.google_ad_click, 0) AS google_ad_click,
  COALESCE(s.bing_tagged_sap, 0) AS bing_tagged_sap,
  COALESCE(s.bing_search_with_ads, 0) AS bing_search_with_ads,
  COALESCE(s.bing_ad_click, 0) AS bing_ad_click,
  COALESCE(r.retained_4_weeks_from_submission_date, FALSE) AS retained_4_weeks_from_submission_date,
  COALESCE(st.sponsored_tile_clicks, 0) AS sponsored_tile_clicks
FROM
  fenix_clients_daily f
LEFT JOIN
  fenix_metrics_daily m
  USING (client_id, submission_date)
LEFT JOIN
  search_clients_daily s
  USING (client_id, submission_date)
LEFT JOIN
  retention_clients_daily r
  USING (client_id, submission_date)
LEFT JOIN
  sponsored_tile_daily st
  USING (client_id, submission_date)
