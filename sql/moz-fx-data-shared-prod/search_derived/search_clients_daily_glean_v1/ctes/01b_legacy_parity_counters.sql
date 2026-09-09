
-- legacy_parity_counters_cte
-- The v8-comparable counters, read from the same metrics ping partition
-- clients_with_adblocker_addons_cte already scans. What they measure, and how they compare
-- to v8 and to the SERP-derived columns, is in the README.
--
-- The three families are UNPIVOTed into one long row set keyed by access point so the
-- 17 columns per family do not become 51 near-identical UNNESTs.
WITH legacy_raw_cte AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    sample_id,
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
legacy_exploded_cte AS (
  SELECT
    legacy_raw_cte.client_id,
    legacy_raw_cte.submission_date,
    legacy_raw_cte.sample_id,
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
    legacy_raw_cte,
    UNNEST(legacy_raw_cte.families) AS f,
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
legacy_parity_counters_cte AS (
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
)
-- the file runs standalone, like 01_adblocker.sql; query.sql drops this trailing SELECT and
-- takes the six CTEs above as ordinary members of its own WITH chain
SELECT
  *
FROM
  legacy_parity_counters_cte
