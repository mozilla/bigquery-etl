
-- legacy_base_cte
-- The legacy row set, read once from the same metrics ping partition
-- clients_with_adblocker_addons_cte already scans. Every legacy CTE below reads this rather than
-- the source, the way sap_base_cte and serp_base_cte serve their own pipelines. What the counters
-- measure, and how they compare to v8 and to the SERP-derived columns, is in the README.
--
-- The three families are collected into one array of (access_point, family, counter) structs so
-- the 17 columns per family do not become 51 near-identical UNNESTs; legacy_exploded_cte in
-- 02_legacy_counters.sql unnests it.
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
    -- the metrics ping leaves normalized_app_name NULL on every row, and this table reads one
    -- app, so the literal is what the SAP side's normalization of the same clients returns
    'Firefox' AS normalized_app_name,
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
)
-- the file runs standalone; query.sql drops this trailing SELECT and takes the CTE above as the
-- first member of its own WITH chain
SELECT
  *
FROM
  legacy_base_cte
