-- legacy_counters_agg_cte
-- The six CTEs between legacy_base_cte and the parity-counter tail: one reduces the metrics ping
-- to a client-day row of dimensions, the other five turn the 51 labeled counters into the seven
-- v8-parity measures at the output grain. What they measure is in the README.
-- The tail that attaches the dimensions is 04_legacy_parity_counters.sql, separate because
-- query.sql defines it after clients_with_adblocker_addons_cte, which it reads.
-- one row per client-day, carrying the client dimensions for keys neither pipeline sees.
-- Reads legacy_base_cte rather than the source. Client-day is the finest grain available: the
-- metrics ping has no engine or access point, and the counters it reports are interval totals
-- rather than timestamped events, so there is nothing finer to key on.
-- The latest ping wins. ping_seq is a per-client monotonic counter for this ping type, so it
-- picks the latest ping even when submissions arrive out of order; document_id breaks ties.
-- sample_id is deliberately not projected: legacy_counters_agg_cte already carries it, and a
-- second copy would make the name ambiguous after the join.
WITH legacy_with_client_info_cte AS (
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
    `search_derived.search_clients_daily_glean_v1.legacy_base_cte`
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
    `search_derived.search_clients_daily_glean_v1.legacy_base_cte`,
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
)
-- the file runs standalone; query.sql drops this trailing SELECT and takes the six CTEs above as
-- ordinary members of its own WITH chain
SELECT
  *
FROM
  legacy_counters_agg_cte
