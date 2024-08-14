##### PULL SEARCH BEHAVIOR & ENGINES BY CLIENT
WITH desktop_search_data AS (
  SELECT
    submission_date,
    country,
    client_id,
    distribution_id,
    CASE
      WHEN default_search_engine LIKE '%google%'
        THEN "Google"
      WHEN default_search_engine LIKE '%bing%'
        THEN "Bing"
      WHEN default_search_engine LIKE '%ddg%'
        OR default_search_engine LIKE '%duckduckgo%'
        THEN "DuckDuckGo"
      ELSE NULL
    END AS normalized_default_search_engine,
    normalized_engine,
    SUM(sap) AS search_count,
    SUM(ad_click) AS ad_click
  FROM
    `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    country,
    client_id,
    distribution_id,
    normalized_default_search_engine,
    normalized_engine
),
## have to pull mobile search data from baseline ping so dates match DAU
mobile_baseline_engine AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.search_default_engine_code AS default_search_engine
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
  UNION ALL
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.search_default_engine AS default_search_engine
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
  UNION ALL
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.browser_default_search_engine AS default_search_engine
  FROM
    `moz-fx-data-shared-prod.focus_android.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
  UNION ALL
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.search_default_engine AS default_search_engine
  FROM
    `moz-fx-data-shared-prod.focus_ios.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
mobile_baseline_search AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.search_default_engine_code AS default_search_engine,
    key_value.key AS engine,
    SUM(key_value.value) AS search_count
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`,
    UNNEST(metrics.labeled_counter.metrics_search_count) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  GROUP BY
    1,
    2,
    3,
    4
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.search_default_engine AS default_search_engine,
    key_value.key AS engine,
    SUM(key_value.value) AS search_count
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline`,
    UNNEST(metrics.labeled_counter.search_counts) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  GROUP BY
    1,
    2,
    3,
    4
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.browser_default_search_engine AS default_search_engine,
    key_value.key AS engine,
    SUM(key_value.value) AS search_count
  FROM
    `moz-fx-data-shared-prod.focus_android.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_search_count) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  GROUP BY
    1,
    2,
    3,
    4
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.search_default_engine AS default_search_engine,
    key_value.key AS engine,
    SUM(key_value.value) AS search_count
  FROM
    `moz-fx-data-shared-prod.focus_ios.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_search_count) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  GROUP BY
    1,
    2,
    3,
    4
),
mobile_baseline_search_ad_clicks AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.search_default_engine_code AS default_search_engine,
    key_value.key AS engine,
    SUM(key_value.value) AS ad_click
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  GROUP BY
    1,
    2,
    3,
    4
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.search_default_engine AS default_search_engine,
    key_value.key AS engine,
    SUM(key_value.value) AS ad_click
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  GROUP BY
    1,
    2,
    3,
    4
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.browser_default_search_engine AS default_search_engine,
    key_value.key AS engine,
    SUM(key_value.value) AS ad_click
  FROM
    `moz-fx-data-shared-prod.focus_android.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  GROUP BY
    1,
    2,
    3,
    4
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.search_default_engine AS default_search_engine,
    key_value.key AS engine,
    SUM(key_value.value) AS ad_click
  FROM
    `moz-fx-data-shared-prod.focus_ios.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  GROUP BY
    1,
    2,
    3,
    4
),
mobile_baseline_full AS (
  SELECT
    submission_date,
    client_id,
    CASE
      WHEN default_search_engine LIKE '%google%'
        THEN "Google"
      WHEN default_search_engine LIKE '%bing%'
        THEN "Bing"
      WHEN default_search_engine LIKE '%ddg%'
        OR default_search_engine LIKE '%duckduckgo%'
        THEN "DuckDuckGo"
      ELSE NULL
    END AS normalized_default_search_engine,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(engine) AS normalized_engine,
    SUM(search_count) AS search_count,
    SUM(ad_click) AS ad_click
  FROM
    mobile_baseline_engine
  LEFT JOIN
    mobile_baseline_search
    USING (submission_date, client_id, default_search_engine)
  LEFT JOIN
    mobile_baseline_search_ad_clicks
    USING (submission_date, client_id, default_search_engine, engine)
  GROUP BY
    submission_date,
    client_id,
    normalized_default_search_engine,
    normalized_engine
),
### PULL CLIENTS WHO QUALIFY FOR KPI ACTIVITY STANDARDS
desktop_dau_data AS (
  SELECT DISTINCT
    "desktop" AS device,
    submission_date,
    country,
    client_id,
    normalized_channel
  FROM
    `mozdata.telemetry.desktop_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
    # not including Mozilla Online
    AND app_name = "Firefox Desktop"
),
mobile_dau_data AS (
  SELECT DISTINCT
    "mobile" AS device,
    submission_date,
    country,
    client_id,
    normalized_channel
  FROM
    `mozdata.telemetry.mobile_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
    # not including Fenix MozillaOnline, BrowserStack, Klar
    AND app_name IN ("Focus iOS", "Firefox iOS", "Fenix", "Focus Android")
),
### FINAL CLIENT-LEVEL TABLES
desktop_by_client_id AS (
  SELECT DISTINCT
    submission_date,
    device,
    normalized_channel,
    country,
    distribution_id,
    normalized_default_search_engine,
    normalized_engine,
    client_id,
    CASE
      WHEN search_count > 0
        THEN 1
      ELSE 0
    END AS sap_category,
    CASE
      WHEN ad_click > 0
        THEN 1
      ELSE 0
    END AS ad_click_category
  FROM
    desktop_dau_data
  LEFT JOIN
    desktop_search_data
    USING (submission_date, country, client_id)
),
mobile_by_client_id AS (
  SELECT DISTINCT
    submission_date,
    device,
    normalized_channel,
    country,
    "NULL" AS distribution_id,
    normalized_default_search_engine,
    normalized_engine,
    client_id,
    CASE
      WHEN search_count > 0
        THEN 1
      ELSE 0
    END AS sap_category,
    CASE
      WHEN ad_click > 0
        THEN 1
      ELSE 0
    END AS ad_click_category
  FROM
    mobile_dau_data
  LEFT JOIN
    mobile_baseline_full
    USING (submission_date, client_id)
)
### COUNT DAU BY SEARCH BEHAVIOR
SELECT
  "Google" AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  COUNT(
    DISTINCT IF(
      normalized_default_search_engine = "Google"
      AND (
        (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
        OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
      ),
      client_id,
      NULL
    )
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(
      sap_category > 0
      AND normalized_engine = 'Google'
      AND (
        (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
        OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
      ),
      client_id,
      NULL
    )
  ) AS dau_engaged_w_sap,
FROM
  desktop_by_client_id
GROUP BY
  partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category
UNION ALL
SELECT
  "Bing" AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = "Bing", client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(sap_category > 0 AND normalized_engine = 'Bing', client_id, NULL)
  ) AS dau_engaged_w_sap,
FROM
  desktop_by_client_id
WHERE
  (distribution_id IS NULL OR distribution_id NOT LIKE '%acer%')
  AND client_id NOT IN (SELECT client_id FROM `moz-fx-data-shared-prod.search.acer_cohort`)
GROUP BY
  partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category
UNION ALL
SELECT
  "DuckDuckGo" AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = "DuckDuckGo", client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(sap_category > 0 AND normalized_engine = "DuckDuckGo", client_id, NULL)
  ) AS dau_engaged_w_sap
FROM
  desktop_by_client_id
GROUP BY
  partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category
UNION ALL
SELECT
  "Google" AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  # custom engine bug merged in v121
  # null engine bug merged in v126
  # remove default engine data prior to June 2024
  COUNT(
    DISTINCT IF(
      submission_date >= "2024-06-01"
      AND normalized_default_search_engine = "Google"
      AND (
        (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
        OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
      ),
      client_id,
      NULL
    )
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(
      sap_category > 0
      AND normalized_engine = 'Google'
      AND (
        (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
        OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
      ),
      client_id,
      NULL
    )
  ) AS dau_engaged_w_sap
FROM
  mobile_by_client_id
GROUP BY
  partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category
UNION ALL
SELECT
  "Bing" AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  # custom engine bug merged in v121
  # null engine bug merged in v126
  # remove default engine data prior to June 2024
  COUNT(
    DISTINCT IF(
      submission_date >= "2024-06-01"
      AND normalized_default_search_engine = "Bing",
      client_id,
      NULL
    )
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(sap_category > 0 AND normalized_engine = 'Bing', client_id, NULL)
  ) AS dau_engaged_w_sap
FROM
  mobile_by_client_id
GROUP BY
  partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category
UNION ALL
SELECT
  "DuckDuckGo" AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  # custom engine bug merged in v121
  # null engine bug merged in v126
  # remove default engine data prior to June 2024
  COUNT(
    DISTINCT IF(
      submission_date >= "2024-06-01"
      AND normalized_default_search_engine = "DuckDuckGo",
      client_id,
      NULL
    )
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(sap_category > 0 AND normalized_engine = "DuckDuckGo", client_id, NULL)
  ) AS dau_engaged_w_sap
FROM
  mobile_by_client_id
GROUP BY
  partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category
