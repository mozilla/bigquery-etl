-- One row per client per day on desktop, Firefox Android and Firefox iOS: SAP searches and
-- Suggest clicks, the search KPI's components, mapped by kpi_component_map_v1 as of that day.
-- Desktop SAP comes from search_clients_daily_glean and clicks from urlbar_clients_daily_v1.
-- Mobile SAP and dimensions come from mobile_search_clients_daily_v2, clicks from events_stream.
WITH component_map AS (
  SELECT
    platform,
    result_type,
    component,
    definition_version
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    effective_from <= @submission_date
    AND (effective_to IS NULL OR effective_to > @submission_date)
),
-- Two rows in effect for one type would count its clicks twice.
component_map_check AS (
  SELECT
    IF(
      COUNT(*) > COUNT(DISTINCT CONCAT(platform, ':', result_type)),
      ERROR('kpi_component_map_v1 has overlapping rows for a result type'),
      TRUE
    ) AS ok
  FROM
    component_map
),
-- Per platform, so a change to one platform's rows doesn't change another's version. Over
-- every row started by this date, so closing a row can't lower the version.
definition AS (
  SELECT
    platform,
    MAX(definition_version) AS definition_version
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    effective_from <= @submission_date
  GROUP BY
    platform
),
urlbar AS (
  SELECT
    client_id,
    profile_group_id,
    sample_id,
    is_bot,
    results
  FROM
    `moz-fx-data-shared-prod.search_derived.urlbar_clients_daily_v1`
  WHERE
    submission_date = @submission_date
),
-- urlbar_clients_daily_v1 has no partition_offset, so the urlbar task this run waits on
-- writes the next day. The partition read here comes from the previous run. If that run
-- didn't land, fail rather than write a day with no Suggest clicks and no urlbar-only clients.
urlbar_partition_check AS (
  SELECT
    IF(
      COUNT(*) = 0,
      ERROR(FORMAT('urlbar_clients_daily_v1 has no rows for %t', @submission_date)),
      TRUE
    ) AS ok
  FROM
    urlbar
),
dims AS (
  SELECT
    client_id,
    profile_group_id,
    sample_id,
    country,
    normalized_channel,
    os,
    app_version_major,
    normalized_default_search_engine,
    is_default_browser
  FROM
    `moz-fx-data-shared-prod.search_derived.desktop_client_day_dims_v1`
  WHERE
    submission_date = @submission_date
),
-- A type with no map row is excluded, not failed, when it's in a family that is never search
-- or Suggest (quick actions, autofill, interventions, tips, @-keywords) or when the client is
-- on nightly, aurora (Developer Edition) or beta, where new types turn up first and the clicks
-- are few. Otherwise component is NULL and unmapped_check fails the run: release, esr, Other
-- (mostly release-like builds) and no channel all fail. checks.sql warns on every clicked type
-- with no row, so these still get mapped.
clicked_results AS (
  SELECT
    u.client_id,
    r.result_type,
    COALESCE(
      m.component,
      IF(
        REGEXP_CONTAINS(r.result_type, r'^(action|autofill|intervention|tip|restrict_keyword)_')
        OR COALESCE(d.normalized_channel, 'release') IN ('nightly', 'aurora', 'beta'),
        'excluded',
        NULL
      )
    ) AS component,
    r.clicks
  FROM
    urlbar AS u
  CROSS JOIN
    UNNEST(u.results) AS r
  LEFT JOIN
    component_map AS m
    ON m.platform = 'desktop'
    AND m.result_type = r.result_type
  LEFT JOIN
    dims AS d
    USING (client_id)
  WHERE
    r.clicks > 0
),
-- Fenix records one event per kind of click; a click on any other awesomebar result has its
-- own event and isn't Suggest. iOS records every tap as one event, so the map excludes the
-- rest by type.
mobile_events AS (
  SELECT
    'android' AS platform,
    client_id,
    sample_id,
    COALESCE(
      JSON_VALUE(event_extra, '$.provider'),
      JSON_VALUE(event_extra, '$.card_type')
    ) AS result_type
  FROM
    `moz-fx-data-shared-prod.fenix.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'awesomebar'
    AND event_name IN (
      'sponsored_suggestion_clicked',
      'non_sponsored_suggestion_clicked',
      'optimized_suggestion_card_clicked'
    )
  UNION ALL
  SELECT
    'ios' AS platform,
    client_id,
    sample_id,
    JSON_VALUE(event_extra, '$.type') AS result_type
  FROM
    `moz-fx-data-shared-prod.firefox_ios.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'awesomebar'
    AND event_name = 'search_result_tap'
),
-- Like urlbar_clients_daily_v1, the mobile sources have no date_partition_offset, so the
-- partitions read here come from the previous run. Fail rather than write a day without them.
-- Checks any awesomebar event, not just Suggest clicks: Suggest can be switched off remotely,
-- so a day with no Suggest clicks is possible, but not one with no awesomebar events.
mobile_events_check AS (
  SELECT
    IF(
      COUNTIF(platform = 'android') = 0
      OR COUNTIF(platform = 'ios') = 0,
      ERROR(FORMAT('events_stream has no awesomebar events for an app on %t', @submission_date)),
      TRUE
    ) AS ok
  FROM
    (
      SELECT
        'android' AS platform
      FROM
        `moz-fx-data-shared-prod.fenix.events_stream`
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND event_category = 'awesomebar'
      UNION ALL
      SELECT
        'ios' AS platform
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND event_category = 'awesomebar'
    )
),
mobile_clicked_results AS (
  SELECT
    e.platform,
    e.client_id,
    e.sample_id,
    e.result_type,
    m.component
  FROM
    mobile_events AS e
  LEFT JOIN
    component_map AS m
    ON m.platform = e.platform
    AND m.result_type = e.result_type
),
-- A clicked result type with no map row must fail the run, not fall out of the KPI, unless
-- clicked_results excluded it above.
unmapped_check AS (
  SELECT
    IF(
      COUNT(*) > 0,
      ERROR(
        CONCAT(
          'Result types missing from kpi_component_map_v1: ',
          STRING_AGG(DISTINCT result_type, ', ')
        )
      ),
      TRUE
    ) AS ok
  FROM
    (
      SELECT
        result_type,
        component
      FROM
        clicked_results
      UNION ALL
      SELECT
        CONCAT(platform, ':', COALESCE(result_type, 'NULL')) AS result_type,
        component
      FROM
        mobile_clicked_results
    )
  WHERE
    component IS NULL
),
suggest AS (
  SELECT
    client_id,
    SUM(IF(component = 'suggest_sponsored', clicks, 0)) AS suggest_sponsored,
    SUM(IF(component = 'suggest_nonsponsored', clicks, 0)) AS suggest_nonsponsored,
    SUM(IF(component = 'ai_chat', clicks, 0)) AS ai_chat
  FROM
    clicked_results
  GROUP BY
    client_id
),
urlbar_clients AS (
  SELECT
    u.client_id,
    u.profile_group_id,
    u.sample_id,
    u.is_bot,
    TRUE AS in_urlbar,
    COALESCE(s.suggest_sponsored, 0) AS suggest_sponsored,
    COALESCE(s.suggest_nonsponsored, 0) AS suggest_nonsponsored,
    COALESCE(s.ai_chat, 0) AS ai_chat
  FROM
    urlbar AS u
  LEFT JOIN
    suggest AS s
    USING (client_id)
),
sap AS (
  SELECT
    client_id,
    ANY_VALUE(sample_id) AS sample_id,
    SUM(IF(sap_exclusion_reason IS NULL, sap_counts_total, 0)) AS sap,
    SUM(IF(sap_exclusion_reason IS NOT NULL, sap_counts_total, 0)) AS sap_excluded
  FROM
    `moz-fx-data-shared-prod.search.search_clients_daily_glean`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
),
desktop AS (
  SELECT
    client_id,
    -- urlbar's profile_group_id first: baseline lacks it for some clients.
    COALESCE(u.profile_group_id, d.profile_group_id) AS profile_group_id,
    COALESCE(u.sample_id, s.sample_id, d.sample_id) AS sample_id,
    d.country,
    d.normalized_channel,
    d.os,
    d.app_version_major,
    d.normalized_default_search_engine,
    d.is_default_browser,
    COALESCE(s.sap, 0) AS sap,
    COALESCE(s.sap_excluded, 0) AS sap_excluded,
    COALESCE(u.suggest_sponsored, 0) AS suggest_sponsored,
    COALESCE(u.suggest_nonsponsored, 0) AS suggest_nonsponsored,
    COALESCE(u.ai_chat, 0) AS ai_chat,
    u.is_bot,
    COALESCE(u.in_urlbar, FALSE) AS in_urlbar
  FROM
    sap AS s
  FULL OUTER JOIN
    urlbar_clients AS u
    USING (client_id)
  LEFT JOIN
    dims AS d
    USING (client_id)
),
-- A client can have several rows a day, one per engine and source. Each dimension takes the
-- value on the most rows, with ties going to the last by value. 'unavailable' and '' lose to
-- any real engine.
mobile_search AS (
  SELECT
    IF(os = 'iOS', 'ios', 'android') AS platform,
    client_id,
    ANY_VALUE(sample_id) AS sample_id,
    mozfun.stats.mode_last(ARRAY_AGG(country IGNORE NULLS ORDER BY country)) AS country,
    mozfun.stats.mode_last(ARRAY_AGG(channel IGNORE NULLS ORDER BY channel)) AS normalized_channel,
    MAX(os) AS os,
    MAX(SAFE_CAST(SPLIT(app_version, '.')[SAFE_OFFSET(0)] AS NUMERIC)) AS app_version_major,
    mozfun.stats.mode_last(
      ARRAY_AGG(
        NULLIF(NULLIF(default_search_engine, 'unavailable'), '') IGNORE NULLS
        ORDER BY
          default_search_engine
      )
    ) AS default_search_engine,
    SUM(search_count) AS sap
  FROM
    `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v2`
  WHERE
    submission_date = @submission_date
    AND (
      (normalized_app_name = 'Fenix' AND os = 'Android')
      -- Firefox iOS is labeled Fennec in this table. Glean only: versions before 28 are
      -- legacy core-ping rows, dropped as mobile_search_aggregates_v1 does.
      OR (
        normalized_app_name = 'Fennec'
        AND os = 'iOS'
        AND mozfun.norm.truncate_version(app_version, 'major') >= 28
      )
    )
  GROUP BY
    platform,
    client_id
),
mobile_search_check AS (
  SELECT
    IF(
      COUNTIF(platform = 'android') = 0
      OR COUNTIF(platform = 'ios') = 0,
      ERROR(FORMAT('mobile_search_clients_daily_v2 is missing an app on %t', @submission_date)),
      TRUE
    ) AS ok
  FROM
    mobile_search
),
mobile_suggest AS (
  SELECT
    platform,
    client_id,
    ANY_VALUE(sample_id) AS sample_id,
    COUNTIF(component = 'suggest_sponsored') AS suggest_sponsored,
    COUNTIF(component = 'suggest_nonsponsored') AS suggest_nonsponsored
  FROM
    mobile_clicked_results
  GROUP BY
    platform,
    client_id
),
-- Shredder doesn't delete Fenix or Firefox iOS deletion requests from
-- mobile_search_clients_daily_v2 (DENG-4255), and its regular runs here only cover recent
-- requests. Leave out every client that has ever asked to be deleted, so a backfill can't
-- bring them back.
deleted_mobile_clients AS (
  SELECT
    'android' AS platform,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.fenix.deletion_request`
  WHERE
    DATE(submission_timestamp) >= '2019-01-01'
  UNION DISTINCT
  SELECT
    'ios' AS platform,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.firefox_ios.deletion_request`
  WHERE
    DATE(submission_timestamp) >= '2019-01-01'
),
mobile AS (
  SELECT
    platform,
    client_id,
    COALESCE(s.sample_id, c.sample_id) AS sample_id,
    s.country,
    s.normalized_channel,
    s.os,
    s.app_version_major,
    -- Same substring match as desktop_client_day_dims_v1.
    CASE
      WHEN LOWER(s.default_search_engine) LIKE '%google%'
        THEN 'Google'
      WHEN LOWER(s.default_search_engine) LIKE '%bing%'
        THEN 'Bing'
      WHEN LOWER(s.default_search_engine) LIKE '%ddg%'
        OR LOWER(s.default_search_engine) LIKE '%duckduckgo%'
        THEN 'DuckDuckGo'
      ELSE NULL
    END AS normalized_default_search_engine,
    COALESCE(s.sap, 0) AS sap,
    COALESCE(c.suggest_sponsored, 0) AS suggest_sponsored,
    COALESCE(c.suggest_nonsponsored, 0) AS suggest_nonsponsored
  FROM
    mobile_search AS s
  FULL OUTER JOIN
    mobile_suggest AS c
    USING (platform, client_id)
),
all_platforms AS (
  SELECT
    'desktop' AS platform,
    client_id,
    profile_group_id,
    sample_id,
    country,
    normalized_channel,
    os,
    app_version_major,
    normalized_default_search_engine,
    is_default_browser,
    sap,
    sap_excluded,
    suggest_sponsored,
    suggest_nonsponsored,
    ai_chat,
    is_bot,
    in_urlbar,
    -- Firefox 138 is the SAP probe floor; it applies to Suggest too so both halves match.
    COALESCE(app_version_major >= 138, FALSE) AS meets_population_rules
  FROM
    desktop
  WHERE
    -- Every urlbar user stays, even with nothing counted. A client seen only in
    -- search_clients_daily stays only if it started a search, counted or excluded.
    in_urlbar
    OR sap > 0
    OR sap_excluded > 0
  UNION ALL
  SELECT
    platform,
    client_id,
    CAST(NULL AS STRING) AS profile_group_id,
    sample_id,
    country,
    normalized_channel,
    os,
    app_version_major,
    normalized_default_search_engine,
    CAST(NULL AS BOOLEAN) AS is_default_browser,
    sap,
    -- mobile_search_clients_daily_v2 already zeroes rows over 10,000 searches, so they
    -- can't be measured here.
    0 AS sap_excluded,
    suggest_sponsored,
    suggest_nonsponsored,
    0 AS ai_chat,
    CAST(NULL AS BOOLEAN) AS is_bot,
    FALSE AS in_urlbar,
    -- The KPI has no version floor on mobile.
    TRUE AS meets_population_rules
  FROM
    mobile
  LEFT JOIN
    deleted_mobile_clients AS deleted
    USING (platform, client_id)
  WHERE
    deleted.client_id IS NULL
    AND (sap > 0 OR suggest_sponsored > 0 OR suggest_nonsponsored > 0)
)
SELECT
  @submission_date AS submission_date,
  platform,
  client_id,
  profile_group_id,
  sample_id,
  country,
  normalized_channel,
  os,
  app_version_major,
  normalized_default_search_engine,
  is_default_browser,
  sap,
  sap_excluded,
  suggest_sponsored,
  suggest_nonsponsored,
  ai_chat,
  sap + suggest_sponsored + suggest_nonsponsored + ai_chat AS search_queries,
  is_bot,
  in_urlbar,
  meets_population_rules,
  definition.definition_version
FROM
  all_platforms
LEFT JOIN
  definition
  USING (platform)
CROSS JOIN
  unmapped_check
CROSS JOIN
  urlbar_partition_check
CROSS JOIN
  component_map_check
CROSS JOIN
  mobile_events_check
CROSS JOIN
  mobile_search_check
WHERE
  unmapped_check.ok
  AND urlbar_partition_check.ok
  AND component_map_check.ok
  AND mobile_events_check.ok
  AND mobile_search_check.ok
