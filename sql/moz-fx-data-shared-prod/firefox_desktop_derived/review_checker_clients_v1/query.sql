WITH shopping_metrics AS (
  SELECT
    metrics.uuid.legacy_telemetry_client_id AS legacy_client_id,
    client_info.client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    SUM(
      CASE
        WHEN metrics.counter.shopping_product_page_visits IS NOT NULL
          THEN COALESCE(CAST(metrics.counter.shopping_product_page_visits AS INT64), 0)
        ELSE 0
      END
    ) AS shopping_product_page_visits,
    CASE
      WHEN metrics.boolean.shopping_settings_has_onboarded = TRUE
        AND metrics.boolean.shopping_settings_component_opted_out = FALSE
        THEN 1
      ELSE 0
    END AS is_opt_in,
    CASE
      WHEN metrics.boolean.shopping_settings_component_opted_out = TRUE
        THEN 1
      ELSE 0
    END AS is_opt_out,
    normalized_channel,
    normalized_country_code,
    sample_id
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    legacy_client_id,
    client_id,
    submission_date,
    is_opt_in,
    is_opt_out,
    normalized_channel,
    normalized_country_code,
    sample_id
),
active AS (
  SELECT
    client_id,
    DATE(submission_date) AS submission_date,
    active_hours_sum,
    total_uri_count,
    normalized_channel AS channel,
    CASE
      WHEN dau.active_hours_sum > 0
        AND dau.total_uri_count > 0
        THEN 1
      ELSE 0
    END AS is_fx_dau
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily` AS dau
  WHERE
    submission_date = @submission_date
    AND dau.active_hours_sum > 0
    AND dau.total_uri_count > 0
),
active_agg AS (
  SELECT
    client_id,
    DATE(submission_date) AS submission_date,
    SUM(active_hours_sum) AS active_hours_sum,
    is_fx_dau
  FROM
    active
  GROUP BY
    client_id,
    submission_date,
    is_fx_dau
),
search AS (
  SELECT
    DATE(s.submission_date) AS submission_date,
    s.client_id,
    SUM(sap) AS sap,
    SUM(ad_click) AS ad_click
  FROM
    `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily` AS s
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    client_id
),
joined_data AS (
  SELECT
    sm.submission_date,
    normalized_channel,
    normalized_country_code,
    sm.legacy_client_id,
    sm.client_id,
    sm.sample_id,
    sm.shopping_product_page_visits,
    sm.is_opt_in,
    sm.is_opt_out,
    s.sap,
    s.ad_click,
    active_agg.active_hours_sum,
    active_agg.is_fx_dau
  FROM
    shopping_metrics sm
  LEFT JOIN
    active_agg
    ON active_agg.client_id = sm.legacy_client_id
    AND active_agg.submission_date = sm.submission_date
  LEFT JOIN
    search AS s
    ON s.client_id = sm.legacy_client_id
    AND s.submission_date = sm.submission_date
)
SELECT
  *
FROM
  joined_data
