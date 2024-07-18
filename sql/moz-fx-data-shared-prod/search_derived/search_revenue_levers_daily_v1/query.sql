WITH
-- Google Desktop (search + DAU)
desktop_data_google AS (
  SELECT
    submission_date,
    IF(LOWER(channel) LIKE '%esr%', 'ESR', 'personal') AS channel,
    country,
    COUNT(DISTINCT client_id) AS dau,
    COUNT(
      DISTINCT IF(default_search_engine LIKE '%google%', client_id, NULL)
    ) AS dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND normalized_engine = 'Google'
        AND default_search_engine LIKE '%google%',
        client_id,
        NULL
      )
    ) AS dau_engaged_w_sap,
    SUM(IF(normalized_engine = 'Google', sap, 0)) AS sap,
    SUM(IF(normalized_engine = 'Google', tagged_sap, 0)) AS tagged_sap,
    SUM(IF(normalized_engine = 'Google', tagged_follow_on, 0)) AS tagged_follow_on,
    SUM(IF(normalized_engine = 'Google', search_with_ads, 0)) AS search_with_ads,
    SUM(IF(normalized_engine = 'Google', ad_click, 0)) AS ad_click,
    SUM(IF(normalized_engine = 'Google', organic, 0)) AS organic,
    SUM(IF(normalized_engine = 'Google', ad_click_organic, 0)) AS ad_click_organic,
    SUM(IF(normalized_engine = 'Google', search_with_ads_organic, 0)) AS search_with_ads_organic,
    SUM(IF(normalized_engine = 'Google' AND is_sap_monetizable, sap, 0)) AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND (
      (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
    )
  GROUP BY
    submission_date,
    channel,
    country
  ORDER BY
    submission_date,
    channel,
    country
),
-- Bing Desktop (non-Acer)
desktop_data_bing AS (
  SELECT
    submission_date,
    country,
    COUNT(DISTINCT client_id) AS dau,
    COUNT(
      DISTINCT IF(default_search_engine LIKE '%bing%', client_id, NULL)
    ) AS dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND normalized_engine = 'Bing'
        AND default_search_engine LIKE '%bing%',
        client_id,
        NULL
      )
    ) AS dau_engaged_w_sap,
    SUM(IF(normalized_engine = 'Bing', sap, 0)) AS sap,
    SUM(IF(normalized_engine = 'Bing', tagged_sap, 0)) AS tagged_sap,
    SUM(IF(normalized_engine = 'Bing', tagged_follow_on, 0)) AS tagged_follow_on,
    SUM(IF(normalized_engine = 'Bing', search_with_ads, 0)) AS search_with_ads,
    SUM(IF(normalized_engine = 'Bing', ad_click, 0)) AS ad_click,
    SUM(IF(normalized_engine = 'Bing', organic, 0)) AS organic,
    SUM(IF(normalized_engine = 'Bing', ad_click_organic, 0)) AS ad_click_organic,
    SUM(IF(normalized_engine = 'Bing', search_with_ads_organic, 0)) AS search_with_ads_organic,
    SUM(IF(normalized_engine = 'Bing' AND is_sap_monetizable, sap, 0)) AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND (distribution_id IS NULL OR distribution_id NOT LIKE '%acer%')
    AND client_id NOT IN (SELECT client_id FROM `moz-fx-data-shared-prod.search.acer_cohort`)
  GROUP BY
    submission_date,
    country
  ORDER BY
    submission_date,
    country
),
-- DDG Desktop + Extension
desktop_data_ddg AS (
  SELECT
    submission_date,
    country,
    COUNT(DISTINCT client_id) AS dau,
    COUNT(
      DISTINCT IF(
        (
          (default_search_engine LIKE('%ddg%') OR default_search_engine LIKE('%duckduckgo%'))
          AND NOT default_search_engine LIKE('%addon%')
        ),
        client_id,
        NULL
      )
    ) AS ddg_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        (engine) IN ('ddg', 'duckduckgo')
        AND sap > 0
        AND (
          (default_search_engine LIKE('%ddg%') OR default_search_engine LIKE('%duckduckgo%'))
          AND NOT default_search_engine LIKE('%addon%')
        ),
        client_id,
        NULL
      )
    ) AS ddg_dau_engaged_w_sap,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), sap, 0)) AS ddg_sap,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), tagged_sap, 0)) AS ddg_tagged_sap,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), tagged_follow_on, 0)) AS ddg_tagged_follow_on,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), search_with_ads, 0)) AS ddg_search_with_ads,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), ad_click, 0)) AS ddg_ad_click,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), organic, 0)) AS ddg_organic,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), ad_click_organic, 0)) AS ddg_ad_click_organic,
    SUM(
      IF(engine IN ('ddg', 'duckduckgo'), search_with_ads_organic, 0)
    ) AS ddg_search_with_ads_organic,
    SUM(IF(engine IN ('ddg', 'duckduckgo') AND is_sap_monetizable, sap, 0)) AS ddg_monetizable_sap,
   -- in-content probes not available for addon so these metrics although being here will be zero
    COUNT(
      DISTINCT IF(default_search_engine LIKE('ddg%addon'), client_id, NULL)
    ) AS ddgaddon_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        engine = 'ddg-addon'
        AND sap > 0
        AND default_search_engine LIKE('ddg%addon'),
        client_id,
        NULL
      )
    ) AS ddgaddon_dau_engaged_w_sap,
    SUM(IF(engine IN ('ddg-addon'), sap, 0)) AS ddgaddon_sap,
    SUM(IF(engine IN ('ddg-addon'), tagged_sap, 0)) AS ddgaddon_tagged_sap,
    SUM(IF(engine IN ('ddg-addon'), tagged_follow_on, 0)) AS ddgaddon_tagged_follow_on,
    SUM(IF(engine IN ('ddg-addon'), search_with_ads, 0)) AS ddgaddon_search_with_ads,
    SUM(IF(engine IN ('ddg-addon'), ad_click, 0)) AS ddgaddon_ad_click,
    SUM(IF(engine IN ('ddg-addon'), organic, 0)) AS ddgaddon_organic,
    SUM(IF(engine IN ('ddg-addon'), ad_click_organic, 0)) AS ddgaddon_ad_click_organic,
    SUM(
      IF(engine IN ('ddg-addon'), search_with_ads_organic, 0)
    ) AS ddgaddon_search_with_ads_organic,
    SUM(IF(engine IN ('ddg-addon') AND is_sap_monetizable, sap, 0)) AS ddgaddon_monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    country
  ORDER BY
    submission_date,
    country
),
-- Mobile DAU data -- merging baseline clients to AUA clients
## baseline ping -- mobile default search engine by client id
mobile_baseline_engine AS (
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
    google_dau_engaged_w_sap,
    SUM(IF(normalized_engine = 'Google', sap, 0)) AS sap,
    SUM(IF(normalized_engine = 'Google', tagged_sap, 0)) AS tagged_sap,
    SUM(IF(normalized_engine = 'Google', tagged_follow_on, 0)) AS tagged_follow_on,
    SUM(IF(normalized_engine = 'Google', search_with_ads, 0)) AS search_with_ads,
    SUM(IF(normalized_engine = 'Google', ad_click, 0)) AS ad_click,
    SUM(IF(normalized_engine = 'Google', organic, 0)) AS organic,
    SUM(IF(normalized_engine = 'Google', ad_click_organic, 0)) AS ad_click_organic,
   -- metrics do not exist for mobile
    0 AS search_with_ads_organic,
    0 AS monetizable_sap
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
    SUM(IF(normalized_engine = 'Bing', sap, 0)) AS bing_sap,
    SUM(IF(normalized_engine = 'Bing', tagged_sap, 0)) AS bing_tagged_sap,
    SUM(IF(normalized_engine = 'Bing', tagged_follow_on, 0)) AS bing_tagged_follow_on,
    SUM(IF(normalized_engine = 'Bing', search_with_ads, 0)) AS bing_search_with_ads,
    SUM(IF(normalized_engine = 'Bing', ad_click, 0)) AS bing_ad_click,
    SUM(IF(normalized_engine = 'Bing', organic, 0)) AS bing_organic,
    SUM(IF(normalized_engine = 'Bing', ad_click_organic, 0)) AS bing_ad_click_organic,
   -- metrics do not exist for mobile
    0 AS bing_search_with_ads_organic,
    0 AS bing_monetizable_sap,
    SUM(IF(normalized_engine = 'DuckDuckGo', sap, 0)) AS ddg_sap,
    SUM(IF(normalized_engine = 'DuckDuckGo', tagged_sap, 0)) AS ddg_tagged_sap,
    SUM(IF(normalized_engine = 'DuckDuckGo', tagged_follow_on, 0)) AS ddg_tagged_follow_on,
    SUM(IF(normalized_engine = 'DuckDuckGo', search_with_ads, 0)) AS ddg_search_with_ads,
    SUM(IF(normalized_engine = 'DuckDuckGo', ad_click, 0)) AS ddg_ad_click,
    SUM(IF(normalized_engine = 'DuckDuckGo', organic, 0)) AS ddg_organic,
    SUM(IF(normalized_engine = 'DuckDuckGo', ad_click_organic, 0)) AS ddg_ad_click_organic,
   -- metrics do not exist for mobile
    0 AS ddg_search_with_ads_organic,
    0 AS ddg_monetizable_sap
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
-- combine all desktop and mobile together
SELECT
  submission_date,
  'Google' AS partner,
  'desktop' AS device,
  channel,
  country,
  dau,
  dau_engaged_w_sap,
  sap,
  tagged_sap,
  tagged_follow_on,
  search_with_ads,
  ad_click,
  organic,
  ad_click_organic,
  search_with_ads_organic,
  monetizable_sap,
  dau_w_engine_as_default
FROM
  desktop_data_google
UNION ALL
SELECT
  submission_date,
  'Bing' AS partner,
  'desktop' AS device,
  NULL AS channel,
  country,
  dau,
  dau_engaged_w_sap,
  sap,
  tagged_sap,
  tagged_follow_on,
  search_with_ads,
  ad_click,
  organic,
  ad_click_organic,
  search_with_ads_organic,
  monetizable_sap,
  dau_w_engine_as_default
FROM
  desktop_data_bing
UNION ALL
SELECT
  submission_date,
  'DuckDuckGo' AS partner,
  'desktop' AS device,
  NULL AS channel,
  country,
  dau,
  ddg_dau_engaged_w_sap AS dau_engaged_w_sap,
  ddg_sap AS sap,
  ddg_tagged_sap AS tagged_sap,
  ddg_tagged_follow_on AS tagged_follow_on,
  ddg_search_with_ads AS search_with_ads,
  ddg_ad_click AS ad_click,
  ddg_organic AS organic,
  ddg_ad_click_organic AS ad_click_organic,
  ddg_search_with_ads_organic AS search_with_ads_organic,
  ddg_monetizable_sap AS monetizable_sap,
  ddg_dau_w_engine_as_default AS dau_w_engine_as_default
FROM
  desktop_data_ddg
UNION ALL
SELECT
  submission_date,
  'DuckDuckGo' AS partner,
  'extension' AS device,
  NULL AS channel,
  country,
  dau,
  ddgaddon_dau_engaged_w_sap AS dau_engaged_w_sap,
  ddgaddon_sap AS sap,
  ddgaddon_tagged_sap AS tagged_sap,
  ddgaddon_tagged_follow_on AS tagged_follow_on,
  ddgaddon_search_with_ads AS search_with_ads,
  ddgaddon_ad_click AS ad_click,
  ddgaddon_organic AS organic,
  ddgaddon_ad_click_organic AS ad_click_organic,
  ddgaddon_search_with_ads_organic AS search_with_ads_organic,
  ddgaddon_monetizable_sap AS monetizable_sap,
  ddgaddon_dau_w_engine_as_default AS dau_w_engine_as_default
FROM
  desktop_data_ddg
UNION ALL
SELECT
  submission_date,
  'Google' AS partner,
  'mobile' AS device,
  'n/a' AS channel,
  country,
  google_eligible_dau AS dau,
  google_dau_engaged_w_sap AS dau_engaged_w_sap,
  sap,
  tagged_sap,
  tagged_follow_on,
  search_with_ads,
  ad_click,
  organic,
  ad_click_organic,
  search_with_ads_organic,
  monetizable_sap,
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
  bing_sap,
  bing_tagged_sap,
  bing_tagged_follow_on,
  bing_search_with_ads,
  bing_ad_click,
  bing_organic,
  bing_ad_click_organic,
  bing_search_with_ads_organic,
  bing_monetizable_sap,
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
  ddg_sap,
  ddg_tagged_sap,
  ddg_tagged_follow_on,
  ddg_search_with_ads,
  ddg_ad_click,
  ddg_organic,
  ddg_ad_click_organic,
  ddg_search_with_ads_organic,
  ddg_monetizable_sap,
  # custom engine bug merged in v121
  # null engine bug merged in v126
  # remove default engine data prior to June 2024
  IF(submission_date >= "2024-06-01", ddg_dau_w_engine_as_default, NULL) AS dau_w_engine_as_default
FROM
  mobile_data_bing_ddg
