-- join_sources_cte
SELECT
  -- columns present on more than one side are COALESCEd in the order serp, sap, legacy;
  -- a serp_, sap_ or legacy_ prefix means the value comes from that side only
  -- on the grain keys that last argument is load-bearing. legacy_cte joins FULL OUTER, so a
  -- legacy key matching neither pipeline is kept, and without it here that row would publish a
  -- NULL grain beside populated counters -- every such row collapsing onto one tuple and
  -- breaking grain uniqueness. legacy_cte.partner_code is 'unknown_code' rather than NULL on
  -- orphan ad rows, so no grain key becomes nullable.
  COALESCE(serp_final_cte.client_id, sap_final_cte.client_id, legacy_cte.client_id) AS client_id,
  COALESCE(
    serp_final_cte.submission_date,
    sap_final_cte.submission_date,
    legacy_cte.submission_date
  ) AS submission_date,
  COALESCE(
    serp_final_cte.provider_id,
    sap_final_cte.normalized_engine,
    legacy_cte.normalized_engine
  ) AS provider_id,
  -- sap-only, so no COALESCE and no SERP counterpart to fall back to: SERP carries a single
  -- provider extra, already normalized into provider_id above. NULL on a serp-only row.
  sap_final_cte.sap_provider_id,
  sap_final_cte.sap_provider_name,
  COALESCE(
    serp_final_cte.partner_code,
    sap_final_cte.partner_code,
    legacy_cte.partner_code
  ) AS partner_code,
  COALESCE(
    serp_final_cte.search_access_point,
    sap_final_cte.source,
    legacy_cte.search_access_point
  ) AS search_access_point,
  COALESCE(serp_final_cte.sample_id, sap_final_cte.sample_id, legacy_cte.sample_id) AS sample_id,
  COALESCE(
    serp_final_cte.legacy_telemetry_client_id,
    sap_final_cte.legacy_telemetry_client_id,
    legacy_cte.legacy_telemetry_client_id
  ) AS legacy_telemetry_client_id,
  COALESCE(sap_final_cte.sap_counts_total, 0) AS sap_counts_total,
  COALESCE(
    serp_final_cte.profile_group_id,
    sap_final_cte.profile_group_id,
    legacy_cte.profile_group_id
  ) AS profile_group_id,
  COALESCE(serp_final_cte.country, sap_final_cte.country, legacy_cte.country) AS country,
  COALESCE(
    serp_final_cte.normalized_app_name,
    sap_final_cte.normalized_app_name,
    legacy_cte.normalized_app_name
  ) AS normalized_app_name,
  COALESCE(
    serp_final_cte.app_version,
    sap_final_cte.app_version,
    legacy_cte.app_version
  ) AS app_version,
  COALESCE(
    serp_final_cte.app_major_version,
    sap_final_cte.app_major_version,
    legacy_cte.app_major_version
  ) AS app_major_version,
  COALESCE(
    serp_final_cte.app_minor_version,
    sap_final_cte.app_minor_version,
    legacy_cte.app_minor_version
  ) AS app_minor_version,
  COALESCE(
    serp_final_cte.app_patch_revision,
    sap_final_cte.app_patch_revision,
    legacy_cte.app_patch_revision
  ) AS app_patch_revision,
  COALESCE(serp_final_cte.channel, sap_final_cte.channel, legacy_cte.channel) AS channel,
  COALESCE(
    serp_final_cte.normalized_channel,
    sap_final_cte.normalized_channel,
    legacy_cte.normalized_channel
  ) AS normalized_channel,
  COALESCE(serp_final_cte.locale, sap_final_cte.locale, legacy_cte.locale) AS locale,
  COALESCE(serp_final_cte.os, sap_final_cte.os, legacy_cte.os) AS os,
  COALESCE(
    serp_final_cte.normalized_os,
    sap_final_cte.normalized_os,
    legacy_cte.normalized_os
  ) AS normalized_os,
  COALESCE(
    serp_final_cte.os_version,
    sap_final_cte.os_version,
    legacy_cte.os_version
  ) AS os_version,
  COALESCE(
    serp_final_cte.normalized_os_version,
    sap_final_cte.normalized_os_version,
    legacy_cte.normalized_os_version
  ) AS normalized_os_version,
  COALESCE(
    serp_final_cte.windows_build_number,
    sap_final_cte.windows_build_number,
    legacy_cte.windows_build_number
  ) AS windows_build_number,
  COALESCE(
    serp_final_cte.distribution_id,
    sap_final_cte.distribution_id,
    legacy_cte.distribution_id
  ) AS distribution_id,
  COALESCE(
    serp_final_cte.profile_creation_date,
    sap_final_cte.profile_creation_date,
    legacy_cte.profile_creation_date
  ) AS profile_creation_date,
  COALESCE(
    serp_final_cte.region_home_region,
    sap_final_cte.region_home_region,
    legacy_cte.region_home_region
  ) AS region_home_region,
  -- no legacy fallback: usage.is_default_browser is not sent in the metrics ping, so this stays
  -- NULL on a legacy-only row
  COALESCE(
    serp_final_cte.usage_is_default_browser,
    sap_final_cte.usage_is_default_browser
  ) AS usage_is_default_browser,
  COALESCE(
    serp_final_cte.search_engine_default_display_name,
    sap_final_cte.search_engine_default_display_name,
    legacy_cte.search_engine_default_display_name
  ) AS default_search_engine_display_name,
  COALESCE(
    serp_final_cte.search_engine_default_load_path,
    sap_final_cte.search_engine_default_load_path,
    legacy_cte.search_engine_default_load_path
  ) AS default_search_engine_load_path,
  COALESCE(
    serp_final_cte.search_engine_default_partner_code,
    sap_final_cte.search_engine_default_partner_code,
    legacy_cte.search_engine_default_partner_code
  ) AS default_search_engine_partner_code,
  COALESCE(
    serp_final_cte.search_engine_default_provider_id,
    sap_final_cte.search_engine_default_provider_id,
    legacy_cte.search_engine_default_provider_id
  ) AS default_search_engine_provider_id,
  COALESCE(
    serp_final_cte.search_engine_default_submission_url,
    sap_final_cte.search_engine_default_submission_url,
    legacy_cte.search_engine_default_submission_url
  ) AS default_search_engine_submission_url,
  COALESCE(
    serp_final_cte.search_engine_default_overridden_by_third_party,
    sap_final_cte.search_engine_default_overridden_by_third_party,
    legacy_cte.search_engine_default_overridden_by_third_party
  ) AS default_search_engine_overridden,
  COALESCE(
    serp_final_cte.search_engine_private_display_name,
    sap_final_cte.search_engine_private_display_name,
    legacy_cte.search_engine_private_display_name
  ) AS default_private_search_engine_display_name,
  COALESCE(
    serp_final_cte.search_engine_private_load_path,
    sap_final_cte.search_engine_private_load_path,
    legacy_cte.search_engine_private_load_path
  ) AS default_private_search_engine_load_path,
  COALESCE(
    serp_final_cte.search_engine_private_partner_code,
    sap_final_cte.search_engine_private_partner_code,
    legacy_cte.search_engine_private_partner_code
  ) AS default_private_search_engine_partner_code,
  COALESCE(
    serp_final_cte.search_engine_private_provider_id,
    sap_final_cte.search_engine_private_provider_id,
    legacy_cte.search_engine_private_provider_id
  ) AS default_private_search_engine_provider_id,
  COALESCE(
    serp_final_cte.search_engine_private_submission_url,
    sap_final_cte.search_engine_private_submission_url,
    legacy_cte.search_engine_private_submission_url
  ) AS default_private_search_engine_submission_url,
  COALESCE(
    serp_final_cte.search_engine_private_overridden_by_third_party,
    sap_final_cte.search_engine_private_overridden_by_third_party,
    legacy_cte.search_engine_private_overridden_by_third_party
  ) AS default_private_search_engine_overridden,
  -- sap-only: a per-search event extra, so it has no value at the client-day grain the metrics
  -- ping reports. NULL on a serp-only or legacy-only row.
  sap_final_cte.overridden_by_third_party AS sap_overridden_by_third_party,
  COALESCE(
    serp_final_cte.ping_start_time,
    sap_final_cte.ping_start_time,
    legacy_cte.ping_start_time
  ) AS ping_start_time,
  COALESCE(
    serp_final_cte.ping_end_time,
    sap_final_cte.ping_end_time,
    legacy_cte.ping_end_time
  ) AS ping_end_time,
  COALESCE(serp_final_cte.ping_seq, sap_final_cte.ping_seq, legacy_cte.ping_seq) AS ping_seq,
  -- prefer whichever side recorded enrollments, not merely whichever side exists. an empty
  -- array is not NULL, so without the IF a SERP impression that predates an enrollment
  -- would win over a SAP event that carries it. legacy is reached only where neither pipeline
  -- has the row at all, which is the case its enrollments are for.
  COALESCE(
    IF(ARRAY_LENGTH(serp_final_cte.experiments) = 0, NULL, serp_final_cte.experiments),
    sap_final_cte.experiments,
    legacy_cte.experiments
  ) AS experiments,
  COALESCE(
    serp_final_cte.has_adblocker_addon,
    sap_final_cte.has_adblocker_addon,
    legacy_cte.has_adblocker_addon
  ) AS has_adblocker_addon,
  COALESCE(
    serp_final_cte.policies_is_enterprise,
    sap_final_cte.policies_is_enterprise,
    legacy_cte.policies_is_enterprise
  ) AS policies_is_enterprise,
  serp_final_cte.ad_click_target AS serp_ad_click_target,
  serp_final_cte.ad_blocker_inferred AS serp_ad_blocker_inferred,
  -- serp-only counts: a sap-only row had no matching SERP rows for that key, so 0 not NULL
  COALESCE(
    serp_final_cte.follow_on_searches_tagged_count,
    0
  ) AS serp_follow_on_searches_tagged_count,
  COALESCE(serp_final_cte.searches_tagged_count, 0) AS serp_searches_tagged_count,
  COALESCE(serp_final_cte.searches_organic_count, 0) AS serp_searches_organic_count,
  COALESCE(
    serp_final_cte.searches_with_ads_organic_count,
    0
  ) AS serp_searches_with_ads_organic_count,
  COALESCE(serp_final_cte.searches_with_ads_tagged_count, 0) AS serp_searches_with_ads_tagged_count,
  COALESCE(serp_final_cte.ad_clicks_tagged_count, 0) AS serp_ad_clicks_tagged_count,
  COALESCE(serp_final_cte.ad_clicks_organic_count, 0) AS serp_ad_clicks_organic_count,
  COALESCE(serp_final_cte.num_ad_clicks, 0) AS serp_num_ad_clicks,
  COALESCE(serp_final_cte.num_non_ad_link_clicks, 0) AS serp_num_non_ad_link_clicks,
  COALESCE(serp_final_cte.num_other_engagements, 0) AS serp_num_other_engagements,
  COALESCE(serp_final_cte.num_ads_loaded, 0) AS serp_num_ads_loaded,
  COALESCE(serp_final_cte.num_ads_visible, 0) AS serp_num_ads_visible,
  COALESCE(serp_final_cte.num_ads_blocked, 0) AS serp_num_ads_blocked,
  COALESCE(serp_final_cte.num_ads_notshowing, 0) AS serp_num_ads_notshowing,
  COALESCE(
    serp_final_cte.profile_age_in_days,
    sap_final_cte.profile_age_in_days
  ) AS profile_age_in_days,
  COALESCE(serp_final_cte.counts_total, 0) AS serp_counts_total,
  -- falls back to 0, not NULL, when no side reported it. sap_aggregates_cte casts this integer
  -- counter to float64, so cast back to INT64 to keep the declared INTEGER type; the metrics
  -- ping stores it as INT64 already, so the legacy arm needs no cast.
  COALESCE(
    serp_final_cte.max_concurrent_tab_count_max,
    CAST(sap_final_cte.concurrent_tab_count_max AS INT64),
    legacy_cte.max_concurrent_tab_count_max,
    0
  ) AS max_concurrent_tab_count_max,
  -- legacy-parity counters. 0 not NULL when the metrics ping carried nothing for this
  -- key, matching the convention used for the serp-only counts above.
  COALESCE(legacy_cte.legacy_tagged_sap, 0) AS legacy_tagged_sap,
  COALESCE(legacy_cte.legacy_tagged_follow_on, 0) AS legacy_tagged_follow_on,
  COALESCE(legacy_cte.legacy_organic, 0) AS legacy_organic,
  COALESCE(legacy_cte.legacy_search_with_ads_tagged, 0) AS legacy_search_with_ads_tagged,
  COALESCE(legacy_cte.legacy_search_with_ads_organic, 0) AS legacy_search_with_ads_organic,
  COALESCE(legacy_cte.legacy_ad_click_tagged, 0) AS legacy_ad_click_tagged,
  COALESCE(legacy_cte.legacy_ad_click_organic, 0) AS legacy_ad_click_organic,
FROM
  `search_derived.search_clients_daily_glean_v1.serp_final_cte`
  -- FULL OUTER so sap activity with no matching SERP impression is kept.
  -- Plain equality on all five keys. No grain key is ever NULL, and two NULLs are not a key
  -- match: a both-NULL branch pairs every null-keyed row on one side with every null-keyed row
  -- on the other, a cartesian product where plain equality leaves them one-sided instead.
FULL OUTER JOIN
  `search_derived.search_clients_daily_glean_v1.sap_final_cte`
  ON sap_final_cte.client_id = serp_final_cte.client_id
  AND sap_final_cte.submission_date = serp_final_cte.submission_date
  AND sap_final_cte.normalized_engine = serp_final_cte.provider_id
  AND sap_final_cte.source = serp_final_cte.search_access_point
  AND sap_final_cte.partner_code = serp_final_cte.partner_code
  -- FULL OUTER again, not LEFT: a client can increment browser.search.adclicks on a page
  -- whose SERP impression never registered, and that population is the reason these counters
  -- are carried at all. Plain equality on all five keys, with no both-NULL branch -- every
  -- legacy key is non-null by construction, so a branch could only create fan-out.
FULL OUTER JOIN
  `search_derived.search_clients_daily_glean_v1.legacy_parity_counters_cte` AS legacy_cte
  ON legacy_cte.client_id = COALESCE(serp_final_cte.client_id, sap_final_cte.client_id)
  AND legacy_cte.submission_date = COALESCE(
    serp_final_cte.submission_date,
    sap_final_cte.submission_date
  )
  AND legacy_cte.normalized_engine = COALESCE(
    serp_final_cte.provider_id,
    sap_final_cte.normalized_engine
  )
  AND legacy_cte.search_access_point = COALESCE(
    serp_final_cte.search_access_point,
    sap_final_cte.source
  )
  AND legacy_cte.partner_code = COALESCE(serp_final_cte.partner_code, sap_final_cte.partner_code)
