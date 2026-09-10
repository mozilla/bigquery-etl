
-- legacy_parity_counters_cte
-- The v8-comparable counters, read from the same metrics ping partition
-- clients_with_adblocker_addons_cte already scans. What they measure, and how they compare
-- to v8 and to the SERP-derived columns, is in the README.
--
-- The three families are UNPIVOTed into one long row set keyed by access point so the
-- 17 columns per family do not become 51 near-identical UNNESTs.
WITH legacy_base_cte AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    sample_id,
    document_id,
    -- the client dimensions the metrics ping carries. legacy_with_client_info_cte reduces these
    -- to one row per client-day; legacy_exploded_cte selects none of them, so the UNNEST below
    -- does not fan them out. browser_version_info is computed here for the reason sap_base
    -- computes it there: a SELECT cannot reference an alias from its own list.
    -- Two paths differ from the SAP side, which reads the events ping: the submission URLs are
    -- under metrics.url2 rather than metrics.url, and profile_group_id is spelled
    -- legacy_telemetry_profile_group_id.
    normalized_country_code AS country,
    normalized_app_name,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    client_info.app_channel AS channel,
    client_info.locale,
    client_info.os,
    client_info.os_version,
    client_info.windows_build_number,
    client_info.distribution.name AS distribution_id,
    client_info.first_run_date,
    mozfun.norm.browser_version_info(client_info.app_display_version) AS browser_version_info,
    ping_info.start_time AS ping_start_time,
    ping_info.end_time AS ping_end_time,
    ping_info.seq AS ping_seq,
    ping_info.experiments,
    metrics.uuid.legacy_telemetry_client_id,
    metrics.uuid.legacy_telemetry_profile_group_id AS profile_group_id,
    metrics.boolean.policies_is_enterprise,
    metrics.string.region_home_region,
    metrics.string.search_engine_default_display_name,
    metrics.string.search_engine_default_load_path,
    metrics.string.search_engine_default_partner_code,
    metrics.string.search_engine_default_provider_id,
    metrics.url2.search_engine_default_submission_url,
    metrics.boolean.search_engine_default_overridden_by_third_party,
    metrics.string.search_engine_private_display_name,
    metrics.string.search_engine_private_load_path,
    metrics.string.search_engine_private_partner_code,
    metrics.string.search_engine_private_provider_id,
    metrics.url2.search_engine_private_submission_url,
    metrics.boolean.search_engine_private_overridden_by_third_party,
    metrics.quantity.browser_engagement_max_concurrent_tab_count AS max_concurrent_tab_count_max,
    -- (access_point, family, labeled_counter) triples
    [
      STRUCT(
        'urlbar' AS ap,
        'content' AS fam,
        metrics.labeled_counter.browser_search_content_urlbar AS kv
      ),
      STRUCT(
        'urlbar_handoff',
        'content',
        metrics.labeled_counter.browser_search_content_urlbar_handoff
      ),
      STRUCT(
        'urlbar_persisted',
        'content',
        metrics.labeled_counter.browser_search_content_urlbar_persisted
      ),
      STRUCT(
        'urlbar_searchmode',
        'content',
        metrics.labeled_counter.browser_search_content_urlbar_searchmode
      ),
      STRUCT('tabhistory', 'content', metrics.labeled_counter.browser_search_content_tabhistory),
      STRUCT('unknown', 'content', metrics.labeled_counter.browser_search_content_unknown),
      STRUCT('searchbar', 'content', metrics.labeled_counter.browser_search_content_searchbar),
      STRUCT('contextmenu', 'content', metrics.labeled_counter.browser_search_content_contextmenu),
      STRUCT(
        'contextmenu_visual',
        'content',
        metrics.labeled_counter.browser_search_content_contextmenu_visual
      ),
      STRUCT('reload', 'content', metrics.labeled_counter.browser_search_content_reload),
      STRUCT('about_home', 'content', metrics.labeled_counter.browser_search_content_about_home),
      STRUCT(
        'about_newtab',
        'content',
        metrics.labeled_counter.browser_search_content_about_newtab
      ),
      STRUCT('system', 'content', metrics.labeled_counter.browser_search_content_system),
      STRUCT(
        'webextension',
        'content',
        metrics.labeled_counter.browser_search_content_webextension
      ),
      STRUCT('smartbar', 'content', metrics.labeled_counter.browser_search_content_smartbar),
      STRUCT(
        'aiwindow_assistant',
        'content',
        metrics.labeled_counter.browser_search_content_aiwindow_assistant
      ),
      STRUCT(
        'smartwindow_assistant',
        'content',
        metrics.labeled_counter.browser_search_content_smartwindow_assistant
      ),
      -- withads
      STRUCT('urlbar', 'withads', metrics.labeled_counter.browser_search_withads_urlbar),
      STRUCT(
        'urlbar_handoff',
        'withads',
        metrics.labeled_counter.browser_search_withads_urlbar_handoff
      ),
      STRUCT(
        'urlbar_persisted',
        'withads',
        metrics.labeled_counter.browser_search_withads_urlbar_persisted
      ),
      STRUCT(
        'urlbar_searchmode',
        'withads',
        metrics.labeled_counter.browser_search_withads_urlbar_searchmode
      ),
      STRUCT('tabhistory', 'withads', metrics.labeled_counter.browser_search_withads_tabhistory),
      STRUCT('unknown', 'withads', metrics.labeled_counter.browser_search_withads_unknown),
      STRUCT('searchbar', 'withads', metrics.labeled_counter.browser_search_withads_searchbar),
      STRUCT('contextmenu', 'withads', metrics.labeled_counter.browser_search_withads_contextmenu),
      STRUCT(
        'contextmenu_visual',
        'withads',
        metrics.labeled_counter.browser_search_withads_contextmenu_visual
      ),
      STRUCT('reload', 'withads', metrics.labeled_counter.browser_search_withads_reload),
      STRUCT('about_home', 'withads', metrics.labeled_counter.browser_search_withads_about_home),
      STRUCT(
        'about_newtab',
        'withads',
        metrics.labeled_counter.browser_search_withads_about_newtab
      ),
      STRUCT('system', 'withads', metrics.labeled_counter.browser_search_withads_system),
      STRUCT(
        'webextension',
        'withads',
        metrics.labeled_counter.browser_search_withads_webextension
      ),
      STRUCT('smartbar', 'withads', metrics.labeled_counter.browser_search_withads_smartbar),
      STRUCT(
        'aiwindow_assistant',
        'withads',
        metrics.labeled_counter.browser_search_withads_aiwindow_assistant
      ),
      STRUCT(
        'smartwindow_assistant',
        'withads',
        metrics.labeled_counter.browser_search_withads_smartwindow_assistant
      ),
      -- adclicks
      STRUCT('urlbar', 'adclicks', metrics.labeled_counter.browser_search_adclicks_urlbar),
      STRUCT(
        'urlbar_handoff',
        'adclicks',
        metrics.labeled_counter.browser_search_adclicks_urlbar_handoff
      ),
      STRUCT(
        'urlbar_persisted',
        'adclicks',
        metrics.labeled_counter.browser_search_adclicks_urlbar_persisted
      ),
      STRUCT(
        'urlbar_searchmode',
        'adclicks',
        metrics.labeled_counter.browser_search_adclicks_urlbar_searchmode
      ),
      STRUCT('tabhistory', 'adclicks', metrics.labeled_counter.browser_search_adclicks_tabhistory),
      STRUCT('unknown', 'adclicks', metrics.labeled_counter.browser_search_adclicks_unknown),
      STRUCT('searchbar', 'adclicks', metrics.labeled_counter.browser_search_adclicks_searchbar),
      STRUCT(
        'contextmenu',
        'adclicks',
        metrics.labeled_counter.browser_search_adclicks_contextmenu
      ),
      STRUCT(
        'contextmenu_visual',
        'adclicks',
        metrics.labeled_counter.browser_search_adclicks_contextmenu_visual
      ),
      STRUCT('reload', 'adclicks', metrics.labeled_counter.browser_search_adclicks_reload),
      STRUCT('about_home', 'adclicks', metrics.labeled_counter.browser_search_adclicks_about_home),
      STRUCT(
        'about_newtab',
        'adclicks',
        metrics.labeled_counter.browser_search_adclicks_about_newtab
      ),
      STRUCT('system', 'adclicks', metrics.labeled_counter.browser_search_adclicks_system),
      STRUCT(
        'webextension',
        'adclicks',
        metrics.labeled_counter.browser_search_adclicks_webextension
      ),
      STRUCT('smartbar', 'adclicks', metrics.labeled_counter.browser_search_adclicks_smartbar),
      STRUCT(
        'aiwindow_assistant',
        'adclicks',
        metrics.labeled_counter.browser_search_adclicks_aiwindow_assistant
      ),
      STRUCT(
        'smartwindow_assistant',
        'adclicks',
        metrics.labeled_counter.browser_search_adclicks_smartwindow_assistant
      )
    ] AS families
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
-- one row per client-day, carrying the client dimensions for keys neither pipeline sees.
-- Reads legacy_base_cte rather than the source. Client-day is the finest grain available: the
-- metrics ping has no engine or access point, and the counters it reports are interval totals
-- rather than timestamped events, so there is nothing finer to key on.
-- The latest ping wins. ping_seq is a per-client monotonic counter for this ping type, so it
-- picks the latest ping even when submissions arrive out of order; document_id breaks ties.
-- sample_id is deliberately not projected: legacy_counters_agg_cte already carries it, and a
-- second copy would make the name ambiguous after the join.
legacy_with_client_info_cte AS (
  SELECT
    client_id,
    submission_date,
    legacy_telemetry_client_id,
    profile_group_id,
    country,
    normalized_app_name,
    browser_version_info.version AS app_version,
    browser_version_info.major_version AS app_major_version,
    browser_version_info.minor_version AS app_minor_version,
    browser_version_info.patch_revision AS app_patch_revision,
    channel,
    normalized_channel,
    locale,
    os,
    normalized_os,
    os_version,
    normalized_os_version,
    -- os_version_major and os_version_minor are derived in final_cte, from the coalesced inputs
    windows_build_number,
    distribution_id,
    UNIX_DATE(local_date_of(first_run_date)) AS profile_creation_date,
    region_home_region,
    search_engine_default_display_name,
    search_engine_default_load_path,
    search_engine_default_partner_code,
    search_engine_default_provider_id,
    search_engine_default_submission_url,
    search_engine_default_overridden_by_third_party,
    search_engine_private_display_name,
    search_engine_private_load_path,
    search_engine_private_partner_code,
    search_engine_private_provider_id,
    search_engine_private_submission_url,
    search_engine_private_overridden_by_third_party,
    ping_start_time,
    ping_end_time,
    ping_seq,
    experiments,
    policies_is_enterprise,
    max_concurrent_tab_count_max
  FROM
    legacy_base_cte
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id,
        submission_date
      ORDER BY
        ping_seq DESC,
        document_id
    ) = 1
),
legacy_exploded_cte AS (
  SELECT
    legacy_base_cte.client_id,
    legacy_base_cte.submission_date,
    legacy_base_cte.sample_id,
    f.ap AS search_access_point,
    f.fam AS family,
    -- segment 1: provider, normalized the same way the SAP side normalizes it so the
    -- join key cannot drift
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(
      SPLIT(kv.key, ':')[SAFE_OFFSET(0)]
    ) AS normalized_engine,
    -- segment 2: the measure selector
    SPLIT(kv.key, ':')[SAFE_OFFSET(1)] AS tag_type,
    -- segment 3: partner code. Present on `content` ONLY -- the withads/adclicks keys
    -- are two-segment, so this is NULL for them and they take their partner code from
    -- the rank-1 attribution downstream. Do NOT coalesce the ads families to 'no_code'
    -- here: content rows carry a real code, so a 'no_code' ad row would match nothing
    -- and strand as an ad-only output row.
    IF(
      f.fam = 'content',
      COALESCE(NULLIF(SPLIT(kv.key, ':')[SAFE_OFFSET(2)], ''), 'no_code'),
      NULL
    ) AS partner_code,
    kv.value AS n
  FROM
    legacy_base_cte,
    UNNEST(legacy_base_cte.families) AS f,
    UNNEST(f.kv) AS kv
),
-- content carries partner_code, so it aggregates at the full grain directly
legacy_content_agg_cte AS (
  SELECT
    client_id,
    submission_date,
    sample_id,
    normalized_engine,
    partner_code,
    search_access_point,
    SUM(IF(tag_type = 'tagged', n, 0)) AS legacy_tagged_sap,
    SUM(IF(tag_type = 'tagged-follow-on', n, 0)) AS legacy_tagged_follow_on,
    -- known gap against v8, documented in the README
    SUM(IF(tag_type = 'organic', n, 0)) AS legacy_organic,
    SUM(n) AS content_volume
  FROM
    legacy_exploded_cte
  WHERE
    family = 'content'
  GROUP BY
    client_id,
    submission_date,
    sample_id,
    normalized_engine,
    partner_code,
    search_access_point
),
-- the ads families have NO partner_code, so they aggregate one grain coarser
legacy_ads_agg_cte AS (
  SELECT
    client_id,
    submission_date,
    sample_id,
    normalized_engine,
    search_access_point,
    -- v8's search_with_ads and ad_click both INCLUDE follow-on
    SUM(
      IF(family = 'withads' AND tag_type IN ('tagged', 'tagged-follow-on'), n, 0)
    ) AS legacy_search_with_ads_tagged,
    SUM(IF(family = 'withads' AND tag_type = 'organic', n, 0)) AS legacy_search_with_ads_organic,
    SUM(
      IF(family = 'adclicks' AND tag_type IN ('tagged', 'tagged-follow-on'), n, 0)
    ) AS legacy_ad_click_tagged,
    SUM(IF(family = 'adclicks' AND tag_type = 'organic', n, 0)) AS legacy_ad_click_organic
  FROM
    legacy_exploded_cte
  WHERE
    family IN ('withads', 'adclicks')
  GROUP BY
    client_id,
    submission_date,
    sample_id,
    normalized_engine,
    search_access_point
),
-- rank-1 attribution: pick the highest-volume partner code per key to receive the
-- unlabelled ad counts. Only rows spanning more than one partner code are affected.
legacy_ranked_cte AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id,
        submission_date,
        normalized_engine,
        search_access_point
      -- volume picks the winner; partner_code breaks ties so backfills are reproducible
      ORDER BY
        content_volume DESC,
        partner_code ASC
    ) AS rn
  FROM
    legacy_content_agg_cte
),
legacy_counters_agg_cte AS (
  SELECT
    COALESCE(r.client_id, a.client_id) AS client_id,
    COALESCE(r.submission_date, a.submission_date) AS submission_date,
    COALESCE(r.sample_id, a.sample_id) AS sample_id,
    COALESCE(r.normalized_engine, a.normalized_engine) AS normalized_engine,
  -- orphan ad rows -- ad activity on a key with no content row -- keep a sentinel rather
  -- than being dropped, so totals reconcile. FULL OUTER below is what makes this reachable.
    COALESCE(r.partner_code, 'unknown_code') AS partner_code,
    COALESCE(r.search_access_point, a.search_access_point) AS search_access_point,
    COALESCE(r.legacy_tagged_sap, 0) AS legacy_tagged_sap,
    COALESCE(r.legacy_tagged_follow_on, 0) AS legacy_tagged_follow_on,
    COALESCE(r.legacy_organic, 0) AS legacy_organic,
  -- only rank 1 receives the ad counts; every other partner_code row gets 0, so a plain
  -- SUM over the table is correct by construction
    IF(
      COALESCE(r.rn, 1) = 1,
      COALESCE(a.legacy_search_with_ads_tagged, 0),
      0
    ) AS legacy_search_with_ads_tagged,
    IF(
      COALESCE(r.rn, 1) = 1,
      COALESCE(a.legacy_search_with_ads_organic, 0),
      0
    ) AS legacy_search_with_ads_organic,
    IF(COALESCE(r.rn, 1) = 1, COALESCE(a.legacy_ad_click_tagged, 0), 0) AS legacy_ad_click_tagged,
    IF(COALESCE(r.rn, 1) = 1, COALESCE(a.legacy_ad_click_organic, 0), 0) AS legacy_ad_click_organic
  FROM
    legacy_ranked_cte AS r
  FULL OUTER JOIN
    legacy_ads_agg_cte AS a
    ON r.client_id = a.client_id
    AND r.submission_date = a.submission_date
    AND r.normalized_engine = a.normalized_engine
    AND r.search_access_point = a.search_access_point
),
-- the counters with their client dimensions attached. LEFT JOIN in both cases, so a counter key
-- survives even where the client has no metrics-ping row to describe it: the counters are the
-- reason these rows exist and must not be dropped for want of a dimension. Same shape as the
-- two _is_enterprise_cte joins.
-- In query.sql this CTE sits after clients_with_adblocker_addons_cte rather than with the rest
-- of the legacy block, because a CTE cannot reference one defined below it and the legacy block
-- opens the chain.
-- is_default_browser and overridden_by_third_party are absent by necessity rather than oversight:
-- usage.is_default_browser is not sent in the metrics ping, and overridden_by_third_party is a
-- per-search event extra with no meaning at client-day grain.
legacy_parity_counters_cte AS (
  SELECT
    legacy_counters_agg_cte.*,
    legacy_with_client_info_cte.* EXCEPT (client_id, submission_date),
    -- match the pipelines: a client with no adblocker addon is FALSE, not NULL
    COALESCE(clients_with_adblocker_addons_cte.has_adblocker_addon, FALSE) AS has_adblocker_addon
  FROM
    legacy_counters_agg_cte
  LEFT JOIN
    legacy_with_client_info_cte
    USING (client_id, submission_date)
  LEFT JOIN
    `search_derived.search_clients_daily_glean_v1.clients_with_adblocker_addons_cte`
    USING (client_id, submission_date)
)
-- the file runs standalone, like 01_adblocker.sql; query.sql drops this trailing SELECT and
-- takes the eight CTEs above as ordinary members of its own WITH chain
SELECT
  *
FROM
  legacy_parity_counters_cte
