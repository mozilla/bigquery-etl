-- One row per Firefox Desktop client per day, with one value for each dimension, so the SAP
-- and Suggest halves of the search KPI split the same way.
-- Clients with a search_clients_daily_glean_v1 row get the values search_aggregates_glean_v1
-- picks for them (its client_day_country_cte and client_day_dims_cte, copied with sample_id
-- added).
-- Everyone else gets baseline_clients_daily's values. baseline_clients_daily has no default
-- engine or home region, so those come from the client's metrics ping that day, and are NULL
-- when it sent none.
WITH client_day_country_cte AS (
  -- One country per client-day, as in search_clients_daily_v8. A client's rows can carry
  -- different countries, for example when a VPN toggles. Pick the country on the most rows,
  -- then the one with the latest ping_end_time, then the country code.
  SELECT
    client_id,
    submission_date,
    country
  FROM
    (
      SELECT
        client_id,
        submission_date,
        country,
        COUNT(*) AS n_rows,
        MAX(mozfun.glean.parse_datetime(ping_end_time)) AS last_end_time
      FROM
        `moz-fx-data-shared-prod.search_derived.search_clients_daily_glean_v1`
      WHERE
        submission_date = @submission_date
      GROUP BY
        client_id,
        submission_date,
        country
    )
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id,
        submission_date
      ORDER BY
        n_rows DESC,
        last_end_time DESC,
        country
    ) = 1
),
client_day_dims_cte AS (
  -- One value per client-day for every other dimension. A client's rows can disagree because
  -- each comes from a different source: is_default_browser is only set on SERP and SAP rows,
  -- and app_version changes on release days. Prefer SERP and SAP rows, then the latest
  -- ping_end_time, then the value. IGNORE NULLS skips NULLs, and SAFE_OFFSET returns NULL
  -- when a client is NULL on every row.
  SELECT
    client_id,
    submission_date,
    ANY_VALUE(sample_id) AS sample_id,
    ARRAY_AGG(
      is_default_browser IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        is_default_browser
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS is_default_browser,
    ARRAY_AGG(
      app_version IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        app_version
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS app_version,
    ARRAY_AGG(
      default_search_engine_provider_id IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        default_search_engine_provider_id
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS default_search_engine_provider_id,
    ARRAY_AGG(
      default_search_engine_display_name IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        default_search_engine_display_name
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS default_search_engine_display_name,
    ARRAY_AGG(
      default_private_search_engine_display_name IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        default_private_search_engine_display_name
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS default_private_search_engine_display_name,
    ARRAY_AGG(
      home_region IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        home_region
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS home_region,
    ARRAY_AGG(
      policies_is_enterprise IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        policies_is_enterprise
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS policies_is_enterprise,
    ARRAY_AGG(
      os_version IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        os_version
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS os_version,
    ARRAY_AGG(locale IGNORE NULLS ORDER BY is_event_backed DESC, end_ts DESC, locale LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS locale,
    ARRAY_AGG(
      distribution_id IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        distribution_id
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS distribution_id,
    ARRAY_AGG(channel IGNORE NULLS ORDER BY is_event_backed DESC, end_ts DESC, channel LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS channel,
    ARRAY_AGG(
      normalized_channel IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        normalized_channel
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS normalized_channel,
    ARRAY_AGG(os IGNORE NULLS ORDER BY is_event_backed DESC, end_ts DESC, os LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS os
  FROM
    (
      SELECT
        client_id,
        submission_date,
        sample_id,
        is_default_browser,
        app_version,
        default_search_engine_provider_id,
        default_search_engine_display_name,
        default_private_search_engine_display_name,
        home_region,
        policies_is_enterprise,
        os_version,
        locale,
        distribution_id,
        channel,
        normalized_channel,
        os,
        (
          COALESCE(sap_counts_total, 0) > 0
          OR COALESCE(serp_counts_total, 0) > 0
        ) AS is_event_backed,
        mozfun.glean.parse_datetime(ping_end_time) AS end_ts
      FROM
        `moz-fx-data-shared-prod.search_derived.search_clients_daily_glean_v1`
      WHERE
        submission_date = @submission_date
    )
  GROUP BY
    client_id,
    submission_date
),
-- Already one row per client-day. Columns are renamed to match the search table's: on
-- 2026-10-20 they agreed for 94-100% of clients seen in both, the gaps being release-day
-- version changes and NULLs. distribution.name is the field the search table reads; this
-- table's own distribution_id is a different metric and agreed for only 11%.
baseline AS (
  SELECT
    client_id,
    submission_date,
    sample_id,
    country,
    locale,
    app_display_version AS app_version,
    -- The search table has the raw client_info.os, which says Darwin where this says Mac.
    IF(normalized_os = 'Mac', 'Darwin', normalized_os) AS os,
    normalized_os_version AS os_version,
    app_channel AS channel,
    normalized_channel,
    `distribution`.name AS distribution_id,
    is_default_browser,
    policies_is_enterprise
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_daily`
  WHERE
    submission_date = @submission_date
),
-- Same-day only: on 2026-10-20 this covered 85% of Suggest KPI clickers with no search row,
-- and a 7-day lookback (97%) would scan about 7 times as much.
metrics_ping AS (
  SELECT
    client_info.client_id,
    ARRAY_AGG(
      metrics.string.search_engine_default_provider_id IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS default_search_engine_provider_id,
    ARRAY_AGG(
      metrics.string.search_engine_default_display_name IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS default_search_engine_display_name,
    ARRAY_AGG(
      metrics.string.search_engine_private_display_name IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS default_private_search_engine_display_name,
    ARRAY_AGG(
      metrics.string.region_home_region IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS home_region
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
  GROUP BY
    client_id
),
dims AS (
  SELECT
    COALESCE(cdd.client_id, b.client_id) AS client_id,
    COALESCE(cdd.sample_id, b.sample_id) AS sample_id,
    CASE
      WHEN cdd.client_id IS NOT NULL
        THEN 'search_clients_daily'
      WHEN m.client_id IS NOT NULL
        THEN 'baseline_and_metrics'
      ELSE 'baseline'
    END AS dims_source,
    IF(cdd.client_id IS NOT NULL, cdc.country, b.country) AS country,
    IF(cdd.client_id IS NOT NULL, cdd.locale, b.locale) AS locale,
    IF(cdd.client_id IS NOT NULL, cdd.app_version, b.app_version) AS app_version,
    IF(cdd.client_id IS NOT NULL, cdd.os, b.os) AS os,
    IF(cdd.client_id IS NOT NULL, cdd.os_version, b.os_version) AS os_version,
    IF(cdd.client_id IS NOT NULL, cdd.channel, b.channel) AS channel,
    IF(
      cdd.client_id IS NOT NULL,
      cdd.normalized_channel,
      b.normalized_channel
    ) AS normalized_channel,
    IF(cdd.client_id IS NOT NULL, cdd.distribution_id, b.distribution_id) AS distribution_id,
    -- search_aggregates_glean_v1 prefers the baseline value here too.
    COALESCE(b.is_default_browser, cdd.is_default_browser) AS is_default_browser,
    IF(
      cdd.client_id IS NOT NULL,
      cdd.policies_is_enterprise,
      b.policies_is_enterprise
    ) AS policies_is_enterprise,
    IF(
      cdd.client_id IS NOT NULL,
      cdd.default_search_engine_provider_id,
      m.default_search_engine_provider_id
    ) AS default_search_engine_provider_id,
    IF(
      cdd.client_id IS NOT NULL,
      cdd.default_search_engine_display_name,
      m.default_search_engine_display_name
    ) AS default_search_engine_display_name,
    IF(
      cdd.client_id IS NOT NULL,
      cdd.default_private_search_engine_display_name,
      m.default_private_search_engine_display_name
    ) AS default_private_search_engine_display_name,
    IF(cdd.client_id IS NOT NULL, cdd.home_region, m.home_region) AS home_region
  FROM
    client_day_dims_cte AS cdd
  LEFT JOIN
    client_day_country_cte AS cdc
    USING (client_id, submission_date)
  FULL OUTER JOIN
    baseline AS b
    USING (client_id, submission_date)
  -- Only fills clients with no search row, and only those that are in baseline.
  LEFT JOIN
    metrics_ping AS m
    ON m.client_id = b.client_id
    AND cdd.client_id IS NULL
)
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  dims_source,
  country,
  locale,
  app_version,
  `mozfun.norm.browser_version_info`(app_version).major_version AS app_version_major,
  os,
  os_version,
  channel,
  normalized_channel,
  distribution_id,
  -- TRUE for an Acer distribution, as in search_aggregates_glean_v1. Never NULL.
  COALESCE(distribution_id LIKE '%acer%', FALSE) AS is_acer_distribution,
  is_default_browser,
  policies_is_enterprise,
  default_search_engine_provider_id,
  default_search_engine_display_name,
  default_private_search_engine_display_name,
  home_region,
  -- Same substring match as search_aggregates_glean_v1.
  CASE
    WHEN LOWER(default_search_engine_provider_id) LIKE '%google%'
      THEN 'Google'
    WHEN LOWER(default_search_engine_provider_id) LIKE '%bing%'
      THEN 'Bing'
    WHEN LOWER(default_search_engine_provider_id) LIKE '%ddg%'
      OR LOWER(default_search_engine_provider_id) LIKE '%duckduckgo%'
      THEN 'DuckDuckGo'
    ELSE NULL
  END AS normalized_default_search_engine
FROM
  dims
