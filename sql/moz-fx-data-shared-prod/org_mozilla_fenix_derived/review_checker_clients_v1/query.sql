WITH ranked_data AS (
  SELECT
    client_info.client_id AS client_id,
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    metrics.counter.shopping_product_page_visits AS shopping_product_page_visits,
    metrics.boolean.shopping_settings_user_has_onboarded AS is_user_onboarded,
    metrics.boolean.shopping_settings_component_opted_out AS is_component_opted_out,
    metrics.boolean.shopping_settings_nimbus_disabled_shopping AS is_nimbus_disabled,
    normalized_channel,
    normalized_country_code,
    sample_id,
    ping_info.experiments AS experiments,
    ROW_NUMBER() OVER (
      PARTITION BY 
        client_info.client_id
      ORDER BY
        submission_timestamp
    ) AS row_num
  FROM
    `moz-fx-data-shared-prod.fenix.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
shopping_metrics AS (
  SELECT
    client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    SUM(
      CASE
        WHEN shopping_product_page_visits IS NOT NULL
          THEN COALESCE(CAST(shopping_product_page_visits AS INT64), 0)
        ELSE 0
      END
    ) AS shopping_product_page_visits,
    CASE
      WHEN is_user_onboarded = TRUE
        AND is_component_opted_out = FALSE
        THEN 1
      ELSE 0
    END AS is_opt_in,
    CASE
      WHEN is_component_opted_out = TRUE
        THEN 1
      ELSE 0
    END AS is_opt_out,
    CASE
      WHEN is_user_onboarded = TRUE
        THEN 1
      ELSE 0
    END AS is_onboarded,
    CASE
      WHEN is_nimbus_disabled = TRUE
        THEN 1
      ELSE 0
    END AS is_nimbus_disabled,
    normalized_channel,
    normalized_country_code,
    sample_id,
    ANY_VALUE(experiments) AS experiments
  FROM
    ranked_data
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND row_num = 1
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
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_info.client_id,
    submission_date
),
ranked_fx_dau AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    CASE
      WHEN LOWER(metadata.isp.name) != 'browserstack'
        THEN 1
      ELSE 0
    END AS is_fx_dau,
    ROW_NUMBER() OVER (PARTITION BY client_info.client_id ORDER BY submission_timestamp) AS row_num
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
fx_dau AS (
  SELECT
    client_id,
    submission_date,
    is_fx_dau
  FROM
    ranked_fx_dau
  WHERE
    row_num = 1
),
search AS (
  SELECT
    submission_date,
    client_id,
    SUM(search_count) AS sap,
    SUM(ad_click) AS ad_click
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND normalized_app_name_os IN ('Firefox Android', 'Firefox Preview')
  GROUP BY
    client_id,
    submission_date
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
    active.active_hours AS active_hours_sum,
    sm.is_onboarded,
    sm.is_nimbus_disabled,
    fx_dau.is_fx_dau
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
  LEFT JOIN
    fx_dau
  ON
    fx_dau.client_id = sm.client_id
    AND fx_dau.submission_date = sm.submission_date
)
SELECT
  *
FROM
  joined_data
