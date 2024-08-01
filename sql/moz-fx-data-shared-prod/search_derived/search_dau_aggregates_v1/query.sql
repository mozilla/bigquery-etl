WITH desktop_search_data AS (
  SELECT
    submission_date,
    country,
    client_id,
    engine,
    normalized_engine,
    distribution_id,
    default_search_engine,
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
    SUM(sap) AS sap,
    SUM(ad_click) AS ad_click
  FROM
    `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    engine,
    normalized_engine,
    default_search_engine,
    normalized_default_search_engine,
    channel,
    country,
    client_id,
    distribution_id
),
mobile_baseline_search AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    key_value.key AS engine,
    metrics.string.search_default_engine_code AS default_search_engine,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(key_value.key) AS normalized_engine,
    key_value.value AS search_count
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`,
    UNNEST(metrics.labeled_counter.metrics_search_count) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    key_value.key AS engine,
    metrics.string.search_default_engine AS default_search_engine,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(key_value.key) AS normalized_engine,
    key_value.value AS search_count
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline`,
    UNNEST(metrics.labeled_counter.search_counts) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    key_value.key AS engine,
    metrics.string.browser_default_search_engine AS default_search_engine,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(key_value.key) AS normalized_engine,
    key_value.value AS search_count
  FROM
    `moz-fx-data-shared-prod.focus_android.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_search_count) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    key_value.key AS engine,
    normalized_country_code AS country,
    metrics.string.search_default_engine AS default_search_engine,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(key_value.key) AS normalized_engine,
    key_value.value AS search_count
  FROM
    `moz-fx-data-shared-prod.focus_ios.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_search_count) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
),
mobile_baseline_search_ad_clicks AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    metrics.string.search_default_engine_code AS default_search_engine,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(key_value.key) AS normalized_engine,
    key_value.value AS ad_click
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    metrics.string.search_default_engine AS default_search_engine,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(key_value.key) AS normalized_engine,
    key_value.value AS ad_click
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    metrics.string.browser_default_search_engine AS default_search_engine,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(key_value.key) AS normalized_engine,
    key_value.value AS ad_click
  FROM
    `moz-fx-data-shared-prod.focus_android.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
  UNION ALL
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    metrics.string.search_default_engine AS default_search_engine,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(key_value.key) AS normalized_engine,
    key_value.value AS ad_click
  FROM
    `moz-fx-data-shared-prod.focus_ios.baseline`,
    UNNEST(metrics.labeled_counter.browser_search_ad_clicks) AS key_value
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND key_value.value <= 10000
),
mobile_ad_click_sap AS (
  SELECT
    mbs.submission_date,
    mbs.client_id,
    mbs.country,
    mbs.engine,
    mbs.default_search_engine,
    mbs.normalized_engine,
    CASE
      WHEN mbs.default_search_engine LIKE '%google%'
        THEN "Google"
      WHEN mbs.default_search_engine LIKE '%bing%'
        THEN "Bing"
      WHEN mbs.default_search_engine LIKE '%ddg%'
        OR mbs.default_search_engine LIKE '%duckduckgo%'
        THEN "DuckDuckGo"
      ELSE NULL
    END AS normalized_default_search_engine,
    NULL AS distribution_id,
    CASE
      WHEN search_count > 0
        THEN 1
      ELSE 0
    END AS sap_day,
    CASE
      WHEN ad_click > 0
        THEN 1
      ELSE 0
    END AS ad_click_day
  FROM
    mobile_baseline_search mbs
  INNER JOIN
    mobile_baseline_search_ad_clicks mac
    ON mac.submission_date = mbs.submission_date
    AND mbs.client_id = mac.client_id
    AND mbs.country = mac.country
),
dau_ids AS (
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
),
## baseline-powered clients who qualify for KPI (activity filters applied)
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
),
desktop_search_users AS (
  SELECT
    device,
    submission_date,
    country,
    engine,
    normalized_channel,
    normalized_engine,
    default_search_engine,
    normalized_default_search_engine,
    client_id,
    distribution_id,
    CASE
      WHEN sap > 0
        THEN 1
      ELSE 0
    END AS sap_day,
    CASE
      WHEN ad_click > 0
        THEN 1
      ELSE 0
    END AS ad_click_day
  FROM
    dau_ids
  LEFT JOIN
    desktop_search_data
    USING (submission_date, country, client_id)
),
final_mobile_dau_counts AS (
  SELECT
    submission_date,
    device,
    mobile_dau_data.country,
    engine,
    normalized_channel,
    normalized_engine,
    default_search_engine,
    normalized_default_search_engine,
    client_id,
    distribution_id,
    sap_day,
    ad_click_day,
    COUNT(DISTINCT client_id) AS eligible_dau,
    COUNT(
      DISTINCT IF(
        (
          (
            submission_date < "2023-12-01"
            AND mobile_dau_data.country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN')
          )
          OR (
            submission_date >= "2023-12-01"
            AND mobile_dau_data.country NOT IN ('RU', 'UA', 'BY', 'CN')
          )
        ),
        client_id,
        NULL
      )
    ) AS google_eligible_dau,
    COUNT(
      DISTINCT IF(
        default_search_engine LIKE '%google%'
        AND (
          (
            submission_date < "2023-12-01"
            AND mobile_dau_data.country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN')
          )
          OR (
            submission_date >= "2023-12-01"
            AND mobile_dau_data.country NOT IN ('RU', 'UA', 'BY', 'CN')
          )
        ),
        client_id,
        NULL
      )
    ) AS google_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap_day > 0
        AND normalized_engine = 'Google'
        AND (
          (
            submission_date < "2023-12-01"
            AND mobile_dau_data.country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN')
          )
          OR (
            submission_date >= "2023-12-01"
            AND mobile_dau_data.country NOT IN ('RU', 'UA', 'BY', 'CN')
          )
        ),
        client_id,
        NULL
      )
    ) AS google_dau_engaged_w_sap,
    COUNT(
      DISTINCT IF(default_search_engine LIKE '%bing%', client_id, NULL)
    ) AS bing_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(sap_day > 0 AND normalized_engine = 'Bing', client_id, NULL)
    ) AS bing_dau_engaged_w_sap,
    COUNT(
      DISTINCT IF(
        default_search_engine LIKE('%ddg%')
        OR default_search_engine LIKE('%duckduckgo%'),
        client_id,
        NULL
      )
    ) AS ddg_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(normalized_engine = "DuckDuckGo" AND sap_day > 0, client_id, NULL)
    ) AS ddg_dau_engaged_w_sap
  FROM
    mobile_dau_data
  LEFT JOIN
    mobile_ad_click_sap
    USING (submission_date, client_id, country)
  GROUP BY
    submission_date,
    device,
    country,
    engine,
    normalized_channel,
    normalized_engine,
    default_search_engine,
    normalized_default_search_engine,
    client_id,
    distribution_id,
    sap_day,
    ad_click_day
)
SELECT
  submission_date,
  "Google" AS partner,
  device,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day,
  COUNT(
    DISTINCT IF(
      (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN')),
      client_id,
      NULL
    )
  ) AS dau_eligible_markets,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = "Google", client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(
      sap_day > 0
      AND normalized_engine = 'Google'
      AND normalized_default_search_engine = "Google",
      client_id,
      NULL
    )
  ) AS dau_engaged_w_sap,
FROM
  desktop_search_users
GROUP BY
  device,
  submission_date,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day
UNION ALL
SELECT
  submission_date,
  "Bing" AS partner,
  device,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day,
  COUNT(DISTINCT client_id) AS dau_eligible_markets,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = "Bing", client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(
      sap_day > 0
      AND normalized_engine = 'Bing'
      AND normalized_default_search_engine = "Bing",
      client_id,
      NULL
    )
  ) AS dau_engaged_w_sap,
FROM
  desktop_search_users
WHERE
  (distribution_id IS NULL OR distribution_id NOT LIKE '%acer%')
  AND client_id NOT IN (SELECT client_id FROM `moz-fx-data-shared-prod.search.acer_cohort`)
GROUP BY
  device,
  submission_date,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day
UNION ALL
SELECT
  submission_date,
  "DuckDuckGo" AS partner,
  device,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day,
  COUNT(DISTINCT client_id) AS dau_eligible_markets,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = "DuckDuckGo", client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(
      sap_day > 0
      AND normalized_engine = "DuckDuckGo"
      AND normalized_default_search_engine = "DuckDuckGo",
      client_id,
      NULL
    )
  ) AS dau_engaged_w_sap
FROM
  desktop_search_users
GROUP BY
  device,
  submission_date,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day
UNION ALL
SELECT
  submission_date,
  "Google" AS partner,
  device,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day,
  COUNT(
    DISTINCT IF(
      (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN')),
      client_id,
      NULL
    )
  ) AS dau_eligible_markets,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = "Google", client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(
      sap_day > 0
      AND normalized_engine = 'Google'
      AND normalized_default_search_engine = "Google",
      client_id,
      NULL
    )
  ) AS dau_engaged_w_sap,
FROM
  final_mobile_dau_counts
GROUP BY
  device,
  submission_date,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day
UNION ALL
SELECT
  submission_date,
  "Bing" AS partner,
  device,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day,
  COUNT(DISTINCT client_id) AS dau_eligible_markets,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = "Bing", client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(
      sap_day > 0
      AND normalized_engine = 'Bing'
      AND normalized_default_search_engine = "Bing",
      client_id,
      NULL
    )
  ) AS dau_engaged_w_sap,
FROM
  final_mobile_dau_counts
GROUP BY
  device,
  submission_date,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day
UNION ALL
SELECT
  submission_date,
  "DuckDuckGo" AS partner,
  device,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day,
  COUNT(DISTINCT client_id) AS dau_eligible_markets,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = "DuckDuckGo", client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(
      sap_day > 0
      AND normalized_engine = "DuckDuckGo"
      AND normalized_default_search_engine = "DuckDuckGo",
      client_id,
      NULL
    )
  ) AS dau_engaged_w_sap
FROM
  final_mobile_dau_counts
GROUP BY
  device,
  submission_date,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day
ORDER BY
  device,
  submission_date,
  country,
  engine,
  normalized_channel,
  normalized_engine,
  default_search_engine,
  normalized_default_search_engine,
  sap_day,
  ad_click_day
