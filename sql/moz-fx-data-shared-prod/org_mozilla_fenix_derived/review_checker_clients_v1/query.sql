WITH shopping_metrics AS (
  SELECT
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
      WHEN metrics.boolean.shopping_settings_user_has_onboarded = TRUE
        AND metrics.boolean.shopping_settings_component_opted_out = FALSE
        THEN 1
      ELSE 0
    END AS is_opt_in,
    CASE
      WHEN metrics.boolean.shopping_settings_component_opted_out = TRUE
        THEN 1
      ELSE 0
    END AS is_opt_out,
    CASE
      WHEN metrics.boolean.shopping_settings_user_has_onboarded = TRUE
        THEN 1
      ELSE 0
    END AS is_onboarded,
    CASE
      WHEN metrics.boolean.shopping_settings_nimbus_disabled_shopping = TRUE
        THEN 1
      ELSE 0
    END AS is_nimbus_disabled,
    normalized_channel,
    normalized_country_code,
    sample_id,
    ANY_VALUE(ping_info.experiments) AS experiments,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1` AS m
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id,
    submission_date,
    is_opt_in,
    is_opt_out,
    normalized_channel,
    normalized_country_code,
    sample_id,
    is_onboarded,
    is_nimbus_disabled
),
active AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    COALESCE(SUM(metrics.timespan.glean_baseline_duration.value), 0) / 3600.0 AS active_hours
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`
  WHERE
    DATE(submission_timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  GROUP BY
    client_info.client_id,
    submission_date
),
search AS (
  SELECT
    submission_date,
    client_id,
    search_count AS sap,
    ad_click
  FROM
    `mozdata.search.mobile_search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND normalized_app_name = "Fenix"
),
joined_data AS (
  SELECT
    sm.submission_date,
    normalized_channel,
    normalized_country_code,
    sm.client_id,
    sm.sample_id,
    sm.shopping_product_page_visits,
    sm.experiments,
    sm.is_opt_in,
    sm.is_opt_out,
    s.sap,
    s.ad_click,
    active.active_hours AS active_hours_sum
  FROM
    shopping_metrics sm
  LEFT JOIN
    active
  ON
    active.client_id = sm.client_id
    AND active.submission_date = sm.submission_date
  LEFT JOIN
    search AS s
  ON
    s.client_id = sm.client_id
    AND s.submission_date = sm.submission_date
)
SELECT
  *
FROM
  joined_data;
