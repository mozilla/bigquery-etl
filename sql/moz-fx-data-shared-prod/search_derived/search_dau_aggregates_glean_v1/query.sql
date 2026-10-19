-- Query for search_derived.search_dau_aggregates_glean_v1
--
-- Desktop only, by design. The mobile arm of search_dau_aggregates_v1 already reads Glean, so
-- it is not duplicated here. Mobile rows stay in that table, under device = 'mobile'.
--
-- The eligible-country lists encode contract terms and are ported verbatim from
-- search_dau_aggregates_v1. The date on which the list changes is part of that term.
CREATE TEMP FUNCTION google_eligible_country(submission_date DATE, country STRING) AS (
  (submission_date < '2023-12-01' AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
  OR (submission_date >= '2023-12-01' AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
);

-- The DAU population, from baseline_active_users (Glean client_id). search_dau_aggregates_v1
-- read telemetry.desktop_active_users (legacy client_id). One row per client-day, so country is
-- one value per client-day. app_name leaves out MozillaOnline and BrowserStack.
WITH desktop_dau_data AS (
  SELECT DISTINCT
    'desktop' AS device,
    submission_date,
    country,
    client_id,
    channel AS normalized_channel
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
    AND app_name = 'Firefox Desktop'
),
-- Default engine and distribution for every DAU client, searching or not.
-- search_clients_daily_glean_v1 only has rows for clients with search activity, so they come from
-- the metrics ping. About 8% of DAU send no metrics ping on a given day, and both values rarely
-- change, so the nearest ping from 7 days before to 1 day after is used (the job runs after the
-- next day has landed). provider_id is preferred; engine_id covers builds without it, and the
-- client table is the last fallback.
metrics_ping_attributes AS (
  SELECT
    client_info.client_id AS client_id,
    ARRAY_AGG(
      metrics.string.search_engine_default_provider_id IGNORE NULLS
      ORDER BY
        ABS(DATE_DIFF(DATE(submission_timestamp), @submission_date, DAY)),
        submission_timestamp DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS provider_id,
    ARRAY_AGG(
      metrics.string.search_engine_default_engine_id IGNORE NULLS
      ORDER BY
        ABS(DATE_DIFF(DATE(submission_timestamp), @submission_date, DAY)),
        submission_timestamp DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS engine_id,
    ARRAY_AGG(
      client_info.distribution.name IGNORE NULLS
      ORDER BY
        ABS(DATE_DIFF(DATE(submission_timestamp), @submission_date, DAY)),
        submission_timestamp DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS distribution_id
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
  GROUP BY
    client_id
),
client_table_attributes AS (
  SELECT
    client_id,
    ARRAY_AGG(
      default_search_engine_provider_id IGNORE NULLS
      ORDER BY
        default_search_engine_provider_id
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS provider_id,
    ARRAY_AGG(distribution_id IGNORE NULLS ORDER BY distribution_id LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS distribution_id
  FROM
    `moz-fx-data-shared-prod.search_derived.search_clients_daily_glean_v1`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
),
-- An engine_id starting with 'other' is a user-installed engine (other-<name>), so it is read as
-- 'other' and never matched as Google, Bing or DuckDuckGo. Engine ids and provider ids both hold
-- the engine name (google-b-d, google), so a substring match groups them.
desktop_attributes AS (
  SELECT
    client_id,
    distribution_id,
    CASE
      WHEN LOWER(default_engine) LIKE '%google%'
        THEN 'Google'
      WHEN LOWER(default_engine) LIKE '%bing%'
        THEN 'Bing'
      WHEN LOWER(default_engine) LIKE '%ddg%'
        OR LOWER(default_engine) LIKE '%duckduckgo%'
        THEN 'DuckDuckGo'
      ELSE NULL
    END AS normalized_default_search_engine
  FROM
    (
      SELECT
        dau.client_id,
        COALESCE(mpa.distribution_id, cta.distribution_id) AS distribution_id,
        COALESCE(
          mpa.provider_id,
          IF(STARTS_WITH(LOWER(mpa.engine_id), 'other'), 'other', mpa.engine_id),
          cta.provider_id
        ) AS default_engine
      FROM
        desktop_dau_data AS dau
      LEFT JOIN
        metrics_ping_attributes AS mpa
        ON dau.client_id = mpa.client_id
      LEFT JOIN
        client_table_attributes AS cta
        ON dau.client_id = cta.client_id
    )
),
-- What each client searched, one row per client and engine. Joined on client_id alone, not on
-- country as search_dau_aggregates_v1 did: the two sides no longer share a geo source, so a
-- country join would count a searcher as a non-searcher wherever their countries differ.
desktop_search_data AS (
  SELECT
    client_id,
    normalized_engine,
    SUM(sap_counts_total) AS search_count,
    -- Tagged ad clicks only, like ad_click in search_clients_daily_v8.
    SUM(legacy_ad_clicks_tagged_sum) AS ad_click,
    SUM(serp_ad_clicks_tagged_sum) AS ad_click_serp
  FROM
    `moz-fx-data-shared-prod.search_derived.search_clients_daily_glean_v1`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id,
    normalized_engine
),
desktop_by_client_id AS (
  SELECT DISTINCT
    dau.submission_date,
    dau.device,
    dau.normalized_channel,
    dau.country,
    attr.distribution_id,
    attr.normalized_default_search_engine,
    srch.normalized_engine,
    dau.client_id,
    IF(COALESCE(srch.search_count, 0) > 0, 1, 0) AS sap_category,
    IF(COALESCE(srch.ad_click, 0) > 0, 1, 0) AS ad_click_category,
    IF(COALESCE(srch.ad_click_serp, 0) > 0, 1, 0) AS ad_click_category_serp
  FROM
    desktop_dau_data AS dau
  LEFT JOIN
    desktop_attributes AS attr
    ON dau.client_id = attr.client_id
  LEFT JOIN
    desktop_search_data AS srch
    ON dau.client_id = srch.client_id
)
SELECT
  'Google' AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  ad_click_category_serp,
  COUNT(
    DISTINCT IF(
      normalized_default_search_engine = 'Google'
      AND google_eligible_country(submission_date, country),
      client_id,
      NULL
    )
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(
      sap_category > 0
      AND normalized_engine = 'Google'
      AND google_eligible_country(submission_date, country),
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
  ad_click_category,
  ad_click_category_serp
UNION ALL
SELECT
  'Bing' AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  ad_click_category_serp,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = 'Bing', client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(sap_category > 0 AND normalized_engine = 'Bing', client_id, NULL)
  ) AS dau_engaged_w_sap,
FROM
  desktop_by_client_id
-- DECISION TO BE FINALIZED. search_dau_aggregates_v1 also excluded clients on the
-- search.acer_cohort list, which is keyed on the legacy client_id and so cannot be joined
-- here. Only the distribution check is carried over, so Bing DAU includes the Acer clients
-- that list would have removed. Accepted for now; the exclusion is to be settled before this
-- table feeds revenue reporting.
WHERE
  distribution_id IS NULL
  OR distribution_id NOT LIKE '%acer%'
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
  ad_click_category,
  ad_click_category_serp
UNION ALL
SELECT
  'DuckDuckGo' AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  ad_click_category_serp,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = 'DuckDuckGo', client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(sap_category > 0 AND normalized_engine = 'DuckDuckGo', client_id, NULL)
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
  ad_click_category,
  ad_click_category_serp
