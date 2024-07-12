WITH
-- Google Desktop (search + DAU)
desktop_data_google AS (
  SELECT
    submission_date,
    IF(LOWER(channel) LIKE '%esr%', 'ESR', 'personal') AS channel,
    country,
    COUNT(DISTINCT IF(active_hours_sum > 0 AND total_uri_count > 0, client_id, NULL)) AS dau,
    COUNT(
      DISTINCT IF(
        default_search_engine LIKE '%google%'
        AND active_hours_sum > 0
        AND total_uri_count > 0,
        client_id,
        NULL
      )
    ) AS dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND normalized_engine = 'Google'
        AND default_search_engine LIKE '%google%'
        AND active_hours_sum > 0
        AND total_uri_count > 0,
        client_id,
        NULL
      )
    ) AS dau_engaged_w_sap,
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
    COUNT(DISTINCT IF(active_hours_sum > 0 AND total_uri_count > 0, client_id, NULL)) AS dau,
    COUNT(
      DISTINCT IF(
        default_search_engine LIKE '%bing%'
        AND active_hours_sum > 0
        AND total_uri_count > 0,
        client_id,
        NULL
      )
    ) AS dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND normalized_engine = 'Bing'
        AND default_search_engine LIKE '%bing%'
        AND active_hours_sum > 0
        AND total_uri_count > 0,
        client_id,
        NULL
      )
    ) AS dau_engaged_w_sap
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
    COUNT(DISTINCT IF(active_hours_sum > 0 AND total_uri_count > 0, client_id, NULL)) AS dau,
    COUNT(
      DISTINCT IF(
        (
          (default_search_engine LIKE('%ddg%') OR default_search_engine LIKE('%duckduckgo%'))
          AND NOT default_search_engine LIKE('%addon%')
        )
        AND active_hours_sum > 0
        AND total_uri_count > 0,
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
        )
        AND active_hours_sum > 0
        AND total_uri_count > 0,
        client_id,
        NULL
      )
    ) AS ddg_dau_engaged_w_sap,
    -- in-content probes not available for addon so these metrics although being here will be zero
    COUNT(
      DISTINCT IF(
        default_search_engine LIKE('ddg%addon')
        AND active_hours_sum > 0
        AND total_uri_count > 0,
        client_id,
        NULL
      )
    ) AS ddgaddon_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        engine = 'ddg-addon'
        AND sap > 0
        AND default_search_engine LIKE('ddg%addon')
        AND active_hours_sum > 0
        AND total_uri_count > 0,
        client_id,
        NULL
      )
    ) AS ddgaddon_dau_engaged_w_sap,
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
-- Grab Mobile Eligible DAU
mobile_dau_data AS (
  SELECT
    submission_date,
    country,
    SUM(dau) AS dau
  FROM
    `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
  WHERE
    submission_date = @submission_date
    AND app_name IN ('Fenix', 'Firefox iOS', 'Focus Android', 'Focus iOS')
    AND (
      (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
    )
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
    dau,
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
    ) AS dau_engaged_w_sap
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_engines_sources_daily`
  INNER JOIN
    mobile_dau_data
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
    dau
  ORDER BY
    submission_date,
    country,
    dau
),
-- Bing & DDG Mobile (search only - as mobile search metrics is based on
-- metrics ping, while DAU should be based on main ping on Mobile, see also
-- https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_data_bing_ddg AS (
  SELECT
    submission_date,
    country,
    dau,
    COUNT(
      DISTINCT IF(default_search_engine LIKE '%bing%', client_id, NULL)
    ) AS bing_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND normalized_engine = 'Bing'
        AND default_search_engine LIKE '%bing%',
        client_id,
        NULL
      )
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
      DISTINCT IF(
        (engine) IN ('ddg', 'duckduckgo')
        AND sap > 0
        AND (default_search_engine LIKE('%ddg%') OR default_search_engine LIKE('%duckduckgo%')),
        client_id,
        NULL
      )
    ) AS ddg_dau_engaged_w_sap,
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_engines_sources_daily`
  INNER JOIN
    mobile_dau_data
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
    dau
  ORDER BY
    submission_date,
    country,
    dau
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
  dau,
  dau_engaged_w_sap,
  dau_w_engine_as_default
FROM
  mobile_data_google
UNION ALL
SELECT
  submission_date,
  'Bing' AS partner,
  'mobile' AS device,
  NULL AS channel,
  country,
  dau,
  bing_dau_engaged_w_sap,
  bing_dau_w_engine_as_default AS dau_w_engine_as_default
FROM
  mobile_data_bing_ddg
UNION ALL
SELECT
  submission_date,
  'DuckDuckGo' AS partner,
  'mobile' AS device,
  NULL AS channel,
  country,
  dau,
  ddg_dau_engaged_w_sap,
  ddg_dau_w_engine_as_default AS dau_w_engine_as_default
FROM
  mobile_data_bing_ddg
