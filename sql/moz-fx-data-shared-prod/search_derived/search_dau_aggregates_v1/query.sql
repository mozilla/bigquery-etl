-- Mobile DAU data -- merging baseline clients to AUA clients
## baseline ping -- mobile default search engine by client id
WITH mobile_baseline_engine AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    metrics.string.search_default_engine_code AS default_search_engine,
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
## baseline ping search counts -- mobile search counts by client id
mobile_baseline_search AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
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
## baseline-powered clients who qualify for KPI (activity filters applied)
mobile_dau_data AS (
  SELECT DISTINCT
    submission_date,
    "mobile" AS device,
    country,
    client_id
  FROM
    `mozdata.telemetry.mobile_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
   # not including Fenix MozillaOnline, BrowserStack, Klar
    AND app_name IN ("Focus iOS", "Firefox iOS", "Fenix", "Focus Android")
),
final_mobile_dau_counts AS (
  SELECT
    submission_date,
    country,
    COUNT(DISTINCT client_id) AS eligible_dau,
    COUNT(
      DISTINCT IF(
        (
          (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
          OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
        ),
        client_id,
        NULL
      )
    ) AS google_eligible_dau,
    COUNT(
      DISTINCT IF(
        default_search_engine LIKE '%google%'
        AND (
          (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
          OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
        ),
        client_id,
        NULL
      )
    ) AS google_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        search_count > 0
        AND normalized_engine = 'Google'
        AND (
          (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
          OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
        ),
        client_id,
        NULL
      )
    ) AS google_dau_engaged_w_sap,
    COUNT(
      DISTINCT IF(default_search_engine LIKE '%bing%', client_id, NULL)
    ) AS bing_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(search_count > 0 AND normalized_engine = 'Bing', client_id, NULL)
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
      DISTINCT IF(normalized_engine = "DuckDuckGo" AND search_count > 0, client_id, NULL)
    ) AS ddg_dau_engaged_w_sap
  FROM
    mobile_dau_data
  LEFT JOIN
    mobile_baseline_engine
    USING (submission_date, client_id)
  LEFT JOIN
    mobile_baseline_search
    USING (submission_date, client_id, default_search_engine)
  GROUP BY
    submission_date,
    country
),
-- Google Mobile (search only - as mobile search metrics is based on metrics
-- ping, while DAU should be based on main ping on Mobile, see also
-- https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_data_google AS (
  SELECT
    submission_date,
    country,
    google_eligible_dau,
    google_dau_w_engine_as_default,
    google_dau_engaged_w_sap
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_engines_sources_daily`
  INNER JOIN
    final_mobile_dau_counts
    USING (submission_date, country)
  WHERE
    submission_date = @submission_date
    AND (
      (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
    )
    AND (
      app_name IN ('Fenix', 'Firefox Preview', 'Focus', 'Focus Android Glean', 'Focus iOS Glean')
      OR (app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    submission_date,
    country,
    google_eligible_dau,
    google_dau_w_engine_as_default,
    google_dau_engaged_w_sap
  ORDER BY
    submission_date,
    country
),
-- Bing & DDG Mobile (search only - as mobile search metrics is based on
-- metrics ping, while DAU should be based on main ping on Mobile, see also
-- https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_data_bing_ddg AS (
  SELECT
    submission_date,
    country,
    eligible_dau,
    bing_dau_w_engine_as_default,
    bing_dau_engaged_w_sap,
    ddg_dau_w_engine_as_default,
    ddg_dau_engaged_w_sap,
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_engines_sources_daily`
  INNER JOIN
    final_mobile_dau_counts
    USING (submission_date, country)
  WHERE
    submission_date = @submission_date
    AND (
      app_name IN ('Fenix', 'Firefox Preview', 'Focus', 'Focus Android Glean', 'Focus iOS Glean')
      OR (app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    submission_date,
    country,
    eligible_dau,
    bing_dau_w_engine_as_default,
    bing_dau_engaged_w_sap,
    ddg_dau_w_engine_as_default,
    ddg_dau_engaged_w_sap
  ORDER BY
    submission_date,
    country
)
SELECT
  submission_date,
  'Google' AS partner,
  'mobile' AS device,
  'n/a' AS channel,
  country,
  google_eligible_dau AS dau,
  google_dau_engaged_w_sap AS dau_engaged_w_sap,
  # custom engine bug merged in v121
  # null engine bug merged in v126
  # remove default engine data prior to June 2024
  IF(
    submission_date >= "2024-06-01",
    google_dau_w_engine_as_default,
    NULL
  ) AS dau_w_engine_as_default
FROM
  mobile_data_google
UNION ALL
SELECT
  submission_date,
  'Bing' AS partner,
  'mobile' AS device,
  NULL AS channel,
  country,
  eligible_dau,
  bing_dau_engaged_w_sap,
  # custom engine bug merged in v121
  # null engine bug merged in v126
  # remove default engine data prior to June 2024
  IF(submission_date >= "2024-06-01", bing_dau_w_engine_as_default, NULL) AS dau_w_engine_as_default
FROM
  mobile_data_bing_ddg
UNION ALL
SELECT
  submission_date,
  'DuckDuckGo' AS partner,
  'mobile' AS device,
  NULL AS channel,
  country,
  eligible_dau,
  ddg_dau_engaged_w_sap,
  # custom engine bug merged in v121
  # null engine bug merged in v126
  # remove default engine data prior to June 2024
  IF(submission_date >= "2024-06-01", ddg_dau_w_engine_as_default, NULL) AS dau_w_engine_as_default
FROM
  mobile_data_bing_ddg
