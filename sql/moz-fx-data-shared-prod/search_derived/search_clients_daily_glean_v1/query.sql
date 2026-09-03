-- Query for search_derived.search_clients_daily_glean_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
-- the date portion of a client-local timestamp string. Every value it reads carries a trailing
-- offset, so the date is its first ten characters. Converting to UTC would shift it a day for
-- part of the world, and profile_age_in_days subtracts two of these, so both terms have to be on
-- the same calendar. Parsing no time also absorbs the shapes that carry no seconds component.
CREATE TEMP FUNCTION local_date_of(ts STRING) AS (
  SAFE.PARSE_DATE('%F', SUBSTR(ts, 1, 10))
);

WITH
-- list of ad blocking addons produced using this logic: https://github.com/mozilla/search-adhoc-analysis/tree/master/monetization-blocking-addons
adblocker_addons_cte AS (
  SELECT
    addon_id,
    addon_name
  FROM
    `moz-fx-data-shared-prod.revenue.monetization_blocking_addons`
  WHERE
    blocks_monetization
),
-- this table is the new glean adblocker addons metric ping (used to be legacy telemetry)
clients_with_adblocker_addons_cte AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    TRUE AS has_adblocker_addon
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`,
    UNNEST(JSON_QUERY_ARRAY(metrics.object.addons_active_addons)) AS addons
  INNER JOIN
    adblocker_addons_cte
    ON adblocker_addons_cte.addon_id = JSON_VALUE(addons, '$.id')
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND NOT BOOL(JSON_QUERY(addons, '$.userDisabled'))
    AND NOT BOOL(JSON_QUERY(addons, '$.appDisabled'))
    AND NOT BOOL(JSON_QUERY(addons, '$.blocklisted'))
  GROUP BY
    client_id,
    DATE(submission_timestamp)
),
-- the SAP row set with the three grain keys resolved once. every SAP CTE below reads this
-- rather than the source, so the keys have a single definition and the join keys cannot drift.
-- nothing needs excluding from the passthrough: submission_date, normalized_engine,
-- sap_provider_id, sap_provider_name, partner_code, source and browser_version_info are all
-- names events_stream_v1 does not carry.
sap_base_cte AS (
  SELECT
    *,
    DATE(submission_timestamp) AS submission_date,
    CASE
      WHEN JSON_VALUE(event_extra.provider_id) = 'other'
        THEN `moz-fx-data-shared-prod.udf.normalize_search_engine`(
            JSON_VALUE(event_extra.provider_name)
          )
      ELSE `moz-fx-data-shared-prod.udf.normalize_search_engine`(
          JSON_VALUE(event_extra.provider_id)
        )
    END AS normalized_engine, -- this is "engine" in v8
    -- the two raw extras behind normalized_engine, kept so consumers can see what the engine
    -- CASE collapsed. prefixed here rather than at the join, unlike the README convention:
    -- by join_sap_serp_cte provider_id already means the SERP normalized engine, so an
    -- unprefixed sap provider_id would collide with it
    JSON_VALUE(event_extra.provider_id) AS sap_provider_id,
    JSON_VALUE(event_extra.provider_name) AS sap_provider_name,
    -- partner_code is a grain key, so it must never be NULL: an absent JSON key and an
    -- empty string both become 'no_code' so equality joins match rather than silently missing
    COALESCE(NULLIF(JSON_VALUE(event_extra.partner_code), ''), 'no_code') AS partner_code,
    CASE
      WHEN JSON_VALUE(event_extra.source) = 'abouthome'
        THEN 'about_home'
      WHEN JSON_VALUE(event_extra.source) = 'newtab'
        THEN 'about_newtab'
      ELSE REPLACE(JSON_VALUE(event_extra.source), '-', '_')
    END AS source,
    -- computed once here because a SELECT cannot reference an alias from its own list, so
    -- reading four fields off it downstream would otherwise mean four calls per row. this also
    -- matches the SERP side, where serp_events_v2 stores the struct under the same name
    mozfun.norm.browser_version_info(client_info.app_display_version) AS browser_version_info,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'sap.counts'
),
sap_is_enterprise_cte AS (
    -- we still need this because of document_id is not null
  SELECT
    client_id,
    submission_date,
    -- match v8: statistical mode over the day, ties broken toward the latest event
    mozfun.stats.mode_last(
      ARRAY_AGG(
        CAST(JSON_VALUE(metrics.boolean.policies_is_enterprise, '$') AS boolean)
        ORDER BY
          event_timestamp
      )
    ) AS policies_is_enterprise
  FROM
    sap_base_cte
  WHERE
    document_id IS NOT NULL
  GROUP BY
    client_id,
    submission_date
),
sap_events_with_client_info_cte AS (
  SELECT
    client_id,
    submission_date,
    normalized_engine,
    sap_provider_id,
    sap_provider_name,
    partner_code,
    source,
    sample_id,
    profile_group_id,
    legacy_telemetry_client_id, -- adding this for now so people can join to it if needed
    normalized_country_code AS country,
    normalized_app_name,
    browser_version_info.version AS app_version,
    browser_version_info.major_version AS app_major_version,
    browser_version_info.minor_version AS app_minor_version,
    browser_version_info.patch_revision AS app_patch_revision,
    client_info.app_channel AS channel,
    normalized_channel,
    client_info.locale,
    client_info.os,
    normalized_os,
    client_info.os_version,
    normalized_os_version,
    -- os_version_major and os_version_minor are derived in final_cte, from the coalesced inputs
    client_info.windows_build_number,
    client_info.distribution.name AS distribution_id,
    UNIX_DATE(local_date_of(client_info.first_run_date)) AS profile_creation_date,
    CAST(JSON_VALUE(metrics.string.region_home_region, '$') AS string) AS region_home_region,
    CAST(
      JSON_VALUE(metrics.boolean.usage_is_default_browser, '$') AS boolean
    ) AS usage_is_default_browser,
    CAST(
      JSON_VALUE(metrics.string.search_engine_default_display_name, '$') AS string
    ) AS search_engine_default_display_name,
    CAST(
      JSON_VALUE(metrics.string.search_engine_default_load_path, '$') AS string
    ) AS search_engine_default_load_path,
    CAST(
      JSON_VALUE(metrics.string.search_engine_default_partner_code, '$') AS string
    ) AS search_engine_default_partner_code,
    CAST(
      JSON_VALUE(metrics.string.search_engine_default_provider_id, '$') AS string
    ) AS search_engine_default_provider_id,
    CAST(
      JSON_VALUE(metrics.url.search_engine_default_submission_url, '$') AS string
    ) AS search_engine_default_submission_url,
    CAST(
      JSON_VALUE(metrics.boolean.search_engine_default_overridden_by_third_party, '$') AS boolean
    ) AS search_engine_default_overridden_by_third_party,
    CAST(
      JSON_VALUE(metrics.string.search_engine_private_display_name, '$') AS string
    ) AS search_engine_private_display_name,
    CAST(
      JSON_VALUE(metrics.string.search_engine_private_load_path, '$') AS string
    ) AS search_engine_private_load_path,
    CAST(
      JSON_VALUE(metrics.string.search_engine_private_partner_code, '$') AS string
    ) AS search_engine_private_partner_code,
    CAST(
      JSON_VALUE(metrics.string.search_engine_private_provider_id, '$') AS string
    ) AS search_engine_private_provider_id,
    CAST(
      JSON_VALUE(metrics.url.search_engine_private_submission_url, '$') AS string
    ) AS search_engine_private_submission_url,
    CAST(
      JSON_VALUE(metrics.boolean.search_engine_private_overridden_by_third_party, '$') AS boolean
    ) AS search_engine_private_overridden_by_third_party,
    CAST(
      JSON_VALUE(event_extra.overridden_by_third_party, '$') AS boolean
    ) AS overridden_by_third_party,
    ping_info.start_time AS ping_start_time,
    ping_info.end_time AS ping_end_time,
    ping_info.seq AS ping_seq,
    -- one element per enrollment. ORDER BY makes the element order reproducible, since
    -- JSON_KEYS does not document a stable order
    ARRAY(
      SELECT AS STRUCT
        k AS key,
        STRUCT(
          JSON_VALUE(experiments[k].branch) AS branch,
          STRUCT(
            JSON_VALUE(experiments[k].extra.type) AS type,
            JSON_VALUE(experiments[k].extra.enrollment_id) AS enrollment_id
          ) AS extra
        ) AS value
      FROM
        UNNEST(JSON_KEYS(experiments, 1)) AS k
      ORDER BY
        k
    ) AS experiments
  FROM
    sap_base_cte
    -- this is to get the last instance
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id,
        submission_date,
        normalized_engine,
        partner_code,
        source
      ORDER BY
        event_timestamp DESC,
        event_id -- deterministic tiebreaker (document_id plus event number) so sap passthroughs are reproducible on ties
    ) = 1
),
sap_events_clients_ad_enterprise_cte AS (
  SELECT
    sap_events_with_client_info_cte.*,
    -- match v8: clients with no adblocker addon are FALSE, not NULL
    COALESCE(clients_with_adblocker_addons_cte.has_adblocker_addon, FALSE) AS has_adblocker_addon,
    sap_is_enterprise_cte.policies_is_enterprise
  FROM
    sap_events_with_client_info_cte
  LEFT JOIN
    clients_with_adblocker_addons_cte
    USING (client_id, submission_date)
  LEFT JOIN
    sap_is_enterprise_cte
    USING (client_id, submission_date)
),
sap_aggregates_cte AS (
  SELECT
    client_id,
    submission_date,
    normalized_engine,
    partner_code,
    source,
    -- start_time, not parsed_start_time: same instant, but DATE() of a TIMESTAMP is a UTC date,
    -- which would put this term on a different calendar from the subtrahend
    MAX(UNIX_DATE(local_date_of(ping_info.start_time))) - MAX(
      UNIX_DATE(local_date_of(client_info.first_run_date))
    ) AS profile_age_in_days,
    COUNT(*) AS sap_counts_total,
    MAX(
      CAST(
        JSON_EXTRACT_SCALAR(
          metrics.quantity,
          '$.browser_engagement_max_concurrent_tab_count'
        ) AS float64
      )
    ) AS concurrent_tab_count_max
  FROM
    sap_base_cte
  GROUP BY
    client_id,
    submission_date,
    normalized_engine,
    partner_code,
    source
    -- search_access_point
),
sap_final_cte AS (
  SELECT
    sap_events_clients_ad_enterprise_cte.*,
    sap_aggregates_cte.profile_age_in_days,
    sap_aggregates_cte.sap_counts_total,
    sap_aggregates_cte.concurrent_tab_count_max
  FROM
    sap_events_clients_ad_enterprise_cte
  LEFT JOIN
    sap_aggregates_cte
    USING (client_id, submission_date, normalized_engine, partner_code, source)
    -- using(client_id, submission_date, normalized_engine, partner_code, search_access_point) -- rename
),
-- the SERP row set with the three grain keys resolved once. every SERP CTE below reads this
-- rather than the source, so the keys have a single definition and the join keys cannot drift.
-- the EXCEPT list is the raw form of every key resolved below, dropped so no consumer can key
-- on it by accident -- the same silent-join-miss this base CTE exists to prevent. partner_code
-- would have to be dropped regardless, since the derived column reuses the source name.
-- search_engine stays: normalize_search_engine collapses many raw strings into buckets, so the
-- raw engine is a different fact rather than the same one in another spelling.
serp_base_cte AS (
  SELECT
    * EXCEPT (glean_client_id, partner_code, sap_source),
    glean_client_id AS client_id,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(
      search_engine
    ) AS provider_id, -- this is engine
    -- partner_code is a grain key, so it must never be NULL: an absent map key and an
    -- empty string both become 'no_code' so equality joins match rather than silently missing
    COALESCE(NULLIF(partner_code, ''), 'no_code') AS partner_code,
    -- lowered because serp_events emits 'follow_on_from_refine_on_SERP', the one
    -- mixed-case value in an otherwise lowercase vocabulary
    LOWER(sap_source) AS search_access_point,
  FROM
    `mozdata.firefox_desktop.serp_events`
  WHERE
    submission_date = @submission_date
),
serp_is_enterprise_cte AS (
    -- we still need this because of document_id is not null
  SELECT
    client_id,
    submission_date,
    -- match v8: statistical mode over the day, ties broken toward the latest event
    mozfun.stats.mode_last(
      ARRAY_AGG(policies_is_enterprise ORDER BY event_timestamp)
    ) AS policies_is_enterprise
  FROM
    serp_base_cte
  WHERE
    document_id IS NOT NULL
  GROUP BY
    client_id,
    submission_date
),
serp_events_with_client_info_cte AS (
  SELECT
    client_id,
    submission_date,
    provider_id,
    partner_code,
    search_access_point,
    sample_id,
    profile_group_id,
    legacy_telemetry_client_id,
    normalized_country_code AS country,
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
    usage_is_default_browser,
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
    CAST(overridden_by_third_party AS boolean) AS overridden_by_third_party,
    subsession_start_time AS ping_start_time,
    subsession_end_time AS ping_end_time,
    subsession_counter AS ping_seq,
    experiments
  FROM
    serp_base_cte
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id,
        submission_date,
        provider_id,
        partner_code,
        search_access_point
      ORDER BY
        event_timestamp DESC,
        impression_id -- deterministic tiebreaker (unique serp_events_v2 key) so serp passthroughs are reproducible on ties
    ) = 1
),
serp_events_clients_ad_enterprise_cte AS (
    -- serp_events_clients_ad_enterprise_cte
  SELECT
    serp_events_with_client_info_cte.*,
    -- match v8: clients with no adblocker addon are FALSE, not NULL
    COALESCE(clients_with_adblocker_addons_cte.has_adblocker_addon, FALSE) AS has_adblocker_addon,
    serp_is_enterprise_cte.policies_is_enterprise
  FROM
    serp_events_with_client_info_cte
  LEFT JOIN
    clients_with_adblocker_addons_cte
    USING (client_id, submission_date)
  LEFT JOIN
    serp_is_enterprise_cte
    USING (client_id, submission_date)
),
serp_aggregates_base_cte AS (
  SELECT
    client_id,
    submission_date,
    provider_id,
    partner_code,
    search_access_point,
    -- serp_aggregates_cte flattens this into ad_click_target. concatenating the group's arrays
    -- leaves the row count alone, so the COUNT(*) and SUMs below stay correct; a
    -- CROSS JOIN UNNEST(ad_components) here would multiply rows and corrupt them
    ARRAY_CONCAT_AGG(ad_components) AS ad_components_all,
    LOGICAL_AND(ad_blocker_inferred) AS serp_ad_blocker_inferred,
    COUNTIF(
      (is_tagged IS TRUE)
      AND search_access_point IN (
        'follow_on_from_refine_on_incontent_search',
        'follow_on_from_refine_on_serp'
      )
    ) AS serp_follow_on_searches_tagged_count,
    COUNTIF(is_tagged IS TRUE) AS serp_searches_tagged_count,
    COUNTIF(is_tagged IS TRUE AND num_ads_visible > 0) AS serp_with_ads_tagged_count,
    COUNTIF(is_tagged IS FALSE) AS serp_searches_organic_count,
    COUNTIF(is_tagged IS FALSE AND num_ads_visible > 0) AS serp_with_ads_organic_count,
    SUM(CASE WHEN is_tagged IS TRUE THEN num_ad_clicks ELSE 0 END) AS serp_ad_clicks_tagged_count,
    SUM(CASE WHEN is_tagged IS FALSE THEN num_ad_clicks ELSE 0 END) AS serp_ad_clicks_organic_count,
    SUM(num_ad_clicks) AS num_ad_clicks,
    SUM(num_non_ad_link_clicks) AS num_non_ad_link_clicks,
    SUM(num_other_engagements) AS num_other_engagements,
    SUM(num_ads_loaded) AS num_ads_loaded,
    SUM(num_ads_visible) AS num_ads_visible,
    SUM(num_ads_blocked) AS num_ads_blocked,
    SUM(num_ads_notshowing) AS num_ads_notshowing,
    -- both terms are client-local dates -- see the SAP copy in sap_aggregates_cte for why
    MAX(UNIX_DATE(local_date_of(subsession_start_time))) - MAX(
      UNIX_DATE(local_date_of(first_run_date))
    ) AS profile_age_in_days,
    COUNT(*) AS serp_counts_total,
    MAX(browser_engagement_max_concurrent_tab_count) AS max_concurrent_tab_count_max
  FROM
    serp_base_cte
  GROUP BY
    client_id,
    submission_date,
    provider_id,
    partner_code,
    search_access_point
),
serp_aggregates_cte AS (
  -- Flattens ad_components_all into the ad_click_target string. ARRAY_CONCAT_AGG has to land as
  -- a column in serp_aggregates_base_cte before it can be unnested, because BigQuery rejects an
  -- aggregate inside UNNEST. Reads the base CTE, not serp_events, so there is no second scan.
  -- A key whose ad_components are all empty concatenates to an empty array, which UNNESTs to no
  -- rows and leaves STRING_AGG null.
  -- ORDER BY makes the concatenation order deterministic (STRING_AGG is arbitrary otherwise)
  SELECT
    * EXCEPT (ad_components_all),
    (
      SELECT
        STRING_AGG(DISTINCT ad_component.component, ', ' ORDER BY ad_component.component)
      FROM
        UNNEST(ad_components_all) AS ad_component
    ) AS ad_click_target
  FROM
    serp_aggregates_base_cte
),
serp_final_cte AS (
  SELECT
    serp_events_clients_ad_enterprise_cte.*,
    serp_aggregates_cte.ad_click_target,
    serp_aggregates_cte.serp_ad_blocker_inferred,
    serp_aggregates_cte.serp_follow_on_searches_tagged_count,
    serp_aggregates_cte.serp_searches_tagged_count,
    serp_aggregates_cte.serp_searches_organic_count,
    serp_aggregates_cte.serp_with_ads_organic_count,
    serp_aggregates_cte.serp_with_ads_tagged_count,
    serp_aggregates_cte.serp_ad_clicks_tagged_count,
    serp_aggregates_cte.serp_ad_clicks_organic_count,
    serp_aggregates_cte.num_ad_clicks,
    serp_aggregates_cte.num_non_ad_link_clicks,
    serp_aggregates_cte.num_other_engagements,
    serp_aggregates_cte.num_ads_loaded,
    serp_aggregates_cte.num_ads_visible,
    serp_aggregates_cte.num_ads_blocked,
    serp_aggregates_cte.num_ads_notshowing,
    serp_aggregates_cte.profile_age_in_days,
    serp_aggregates_cte.serp_counts_total,
    serp_aggregates_cte.max_concurrent_tab_count_max
  FROM
    serp_events_clients_ad_enterprise_cte
  LEFT JOIN
    serp_aggregates_cte
    USING (client_id, submission_date, provider_id, partner_code, search_access_point)
),
join_sap_serp_cte AS (
  SELECT
    -- columns present on both sides are COALESCEd with serp taking precedence;
    -- a serp_ or sap_ prefix means the value comes from that side only
    COALESCE(serp_final_cte.client_id, sap_final_cte.client_id) AS client_id,
    COALESCE(serp_final_cte.submission_date, sap_final_cte.submission_date) AS submission_date,
    COALESCE(serp_final_cte.provider_id, sap_final_cte.normalized_engine) AS provider_id,
    -- sap-only, so no COALESCE and no SERP counterpart to fall back to: SERP carries a single
    -- provider extra, already normalized into provider_id above. NULL on a serp-only row.
    sap_final_cte.sap_provider_id,
    sap_final_cte.sap_provider_name,
    COALESCE(serp_final_cte.partner_code, sap_final_cte.partner_code) AS partner_code,
    COALESCE(serp_final_cte.search_access_point, sap_final_cte.source) AS search_access_point,
    COALESCE(serp_final_cte.sample_id, sap_final_cte.sample_id) AS sample_id,
    COALESCE(
      serp_final_cte.legacy_telemetry_client_id,
      sap_final_cte.legacy_telemetry_client_id
    ) AS legacy_telemetry_client_id,
    COALESCE(sap_final_cte.sap_counts_total, 0) AS sap_counts_total,
    COALESCE(serp_final_cte.profile_group_id, sap_final_cte.profile_group_id) AS profile_group_id,
    COALESCE(serp_final_cte.country, sap_final_cte.country) AS country,
    COALESCE(
      serp_final_cte.normalized_app_name,
      sap_final_cte.normalized_app_name
    ) AS normalized_app_name,
    COALESCE(serp_final_cte.app_version, sap_final_cte.app_version) AS app_version,
    COALESCE(
      serp_final_cte.app_major_version,
      sap_final_cte.app_major_version
    ) AS app_major_version,
    COALESCE(
      serp_final_cte.app_minor_version,
      sap_final_cte.app_minor_version
    ) AS app_minor_version,
    COALESCE(
      serp_final_cte.app_patch_revision,
      sap_final_cte.app_patch_revision
    ) AS app_patch_revision,
    COALESCE(serp_final_cte.channel, sap_final_cte.channel) AS channel,
    COALESCE(
      serp_final_cte.normalized_channel,
      sap_final_cte.normalized_channel
    ) AS normalized_channel,
    COALESCE(serp_final_cte.locale, sap_final_cte.locale) AS locale,
    COALESCE(serp_final_cte.os, sap_final_cte.os) AS os,
    COALESCE(serp_final_cte.normalized_os, sap_final_cte.normalized_os) AS normalized_os,
    COALESCE(serp_final_cte.os_version, sap_final_cte.os_version) AS os_version,
    COALESCE(
      serp_final_cte.normalized_os_version,
      sap_final_cte.normalized_os_version
    ) AS normalized_os_version,
    COALESCE(
      serp_final_cte.windows_build_number,
      sap_final_cte.windows_build_number
    ) AS windows_build_number,
    COALESCE(serp_final_cte.distribution_id, sap_final_cte.distribution_id) AS distribution_id,
    COALESCE(
      serp_final_cte.profile_creation_date,
      sap_final_cte.profile_creation_date
    ) AS profile_creation_date,
    COALESCE(
      serp_final_cte.region_home_region,
      sap_final_cte.region_home_region
    ) AS region_home_region,
    COALESCE(
      serp_final_cte.usage_is_default_browser,
      sap_final_cte.usage_is_default_browser
    ) AS usage_is_default_browser,
    COALESCE(
      serp_final_cte.search_engine_default_display_name,
      sap_final_cte.search_engine_default_display_name
    ) AS default_search_engine_display_name,
    COALESCE(
      serp_final_cte.search_engine_default_load_path,
      sap_final_cte.search_engine_default_load_path
    ) AS default_search_engine_load_path,
    COALESCE(
      serp_final_cte.search_engine_default_partner_code,
      sap_final_cte.search_engine_default_partner_code
    ) AS default_search_engine_partner_code,
    COALESCE(
      serp_final_cte.search_engine_default_provider_id,
      sap_final_cte.search_engine_default_provider_id
    ) AS default_search_engine_provider_id,
    COALESCE(
      serp_final_cte.search_engine_default_submission_url,
      sap_final_cte.search_engine_default_submission_url
    ) AS default_search_engine_submission_url,
    COALESCE(
      serp_final_cte.search_engine_default_overridden_by_third_party,
      sap_final_cte.search_engine_default_overridden_by_third_party
    ) AS default_search_engine_overridden,
    COALESCE(
      serp_final_cte.search_engine_private_display_name,
      sap_final_cte.search_engine_private_display_name
    ) AS default_private_search_engine_display_name,
    COALESCE(
      serp_final_cte.search_engine_private_load_path,
      sap_final_cte.search_engine_private_load_path
    ) AS default_private_search_engine_load_path,
    COALESCE(
      serp_final_cte.search_engine_private_partner_code,
      sap_final_cte.search_engine_private_partner_code
    ) AS default_private_search_engine_partner_code,
    COALESCE(
      serp_final_cte.search_engine_private_provider_id,
      sap_final_cte.search_engine_private_provider_id
    ) AS default_private_search_engine_provider_id,
    COALESCE(
      serp_final_cte.search_engine_private_submission_url,
      sap_final_cte.search_engine_private_submission_url
    ) AS default_private_search_engine_submission_url,
    COALESCE(
      serp_final_cte.search_engine_private_overridden_by_third_party,
      sap_final_cte.search_engine_private_overridden_by_third_party
    ) AS default_private_search_engine_overridden,
    COALESCE(
      serp_final_cte.overridden_by_third_party,
      sap_final_cte.overridden_by_third_party
    ) AS overridden_by_third_party,
    COALESCE(serp_final_cte.ping_start_time, sap_final_cte.ping_start_time) AS ping_start_time,
    COALESCE(serp_final_cte.ping_end_time, sap_final_cte.ping_end_time) AS ping_end_time,
    COALESCE(serp_final_cte.ping_seq, sap_final_cte.ping_seq) AS ping_seq,
    -- prefer whichever side recorded enrollments, not merely whichever side exists. an empty
    -- array is not NULL, so without the IF a SERP impression that predates an enrollment
    -- would win over a SAP event that carries it
    COALESCE(
      IF(ARRAY_LENGTH(serp_final_cte.experiments) = 0, NULL, serp_final_cte.experiments),
      sap_final_cte.experiments
    ) AS experiments,
    COALESCE(
      serp_final_cte.has_adblocker_addon,
      sap_final_cte.has_adblocker_addon
    ) AS has_adblocker_addon,
    COALESCE(
      serp_final_cte.policies_is_enterprise,
      sap_final_cte.policies_is_enterprise
    ) AS policies_is_enterprise,
    serp_final_cte.ad_click_target AS serp_ad_click_target,
    serp_final_cte.serp_ad_blocker_inferred AS serp_ad_blocker_inferred,
    -- serp-only counts: a sap-only row had no matching SERP rows for that key, so 0 not NULL
    COALESCE(
      serp_final_cte.serp_follow_on_searches_tagged_count,
      0
    ) AS serp_follow_on_searches_tagged_count,
    COALESCE(serp_final_cte.serp_searches_tagged_count, 0) AS serp_searches_tagged_count,
    COALESCE(serp_final_cte.serp_searches_organic_count, 0) AS serp_searches_organic_count,
    COALESCE(serp_final_cte.serp_with_ads_organic_count, 0) AS serp_with_ads_organic_count,
    COALESCE(serp_final_cte.serp_with_ads_tagged_count, 0) AS serp_with_ads_tagged_count,
    COALESCE(serp_final_cte.serp_ad_clicks_tagged_count, 0) AS serp_ad_clicks_tagged_count,
    COALESCE(serp_final_cte.serp_ad_clicks_organic_count, 0) AS serp_ad_clicks_organic_count,
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
    COALESCE(serp_final_cte.serp_counts_total, 0) AS serp_counts_total,
    -- falls back to 0, not NULL, when neither side reported it. sap_aggregates_cte casts
    -- this integer counter to float64, so cast back to INT64 to keep the declared INTEGER type
    COALESCE(
      serp_final_cte.max_concurrent_tab_count_max,
      CAST(sap_final_cte.concurrent_tab_count_max AS INT64),
      0
    ) AS max_concurrent_tab_count_max
  FROM
    serp_final_cte
    -- FULL OUTER so sap activity with no matching SERP impression is kept.
    -- BigQuery requires a literal `=` in a FULL OUTER JOIN's ON clause, so each key is written
    -- as equality plus an explicit both-NULL match rather than IS NOT DISTINCT FROM.
  FULL OUTER JOIN
    sap_final_cte
    ON (
      sap_final_cte.client_id = serp_final_cte.client_id
      OR (sap_final_cte.client_id IS NULL AND serp_final_cte.client_id IS NULL)
    )
    AND (
      sap_final_cte.submission_date = serp_final_cte.submission_date
      OR (sap_final_cte.submission_date IS NULL AND serp_final_cte.submission_date IS NULL)
    )
    AND (
      sap_final_cte.normalized_engine = serp_final_cte.provider_id
      OR (sap_final_cte.normalized_engine IS NULL AND serp_final_cte.provider_id IS NULL)
    )
    AND (
      sap_final_cte.source = serp_final_cte.search_access_point
      OR (sap_final_cte.source IS NULL AND serp_final_cte.search_access_point IS NULL)
    )
    -- partner_code needs no both-NULL branch: both sides coalesce it to 'no_code'
    AND sap_final_cte.partner_code = serp_final_cte.partner_code
),
final_cte AS (
  SELECT
    submission_date,
    client_id,
    legacy_telemetry_client_id, -- NEW
    provider_id AS normalized_engine,
    search_access_point AS source,
    partner_code, -- NEW
    country,
    normalized_app_name,
    app_version,
    app_major_version,
    app_minor_version,
    app_patch_revision,
    windows_build_number, -- NEW
    distribution_id,
    locale,
    region_home_region AS user_pref_browser_search_region,
    os,
    normalized_os, -- NEW
    os_version,
    normalized_os_version, -- NEW
    channel,
    normalized_channel, -- NEW
    usage_is_default_browser AS is_default_browser,
    profile_creation_date,
    default_search_engine_display_name AS default_search_engine,
    default_search_engine_load_path AS default_search_engine_data_load_path,
    default_search_engine_submission_url AS default_search_engine_data_submission_url,
    default_search_engine_partner_code, -- NEW
    default_search_engine_provider_id, -- NEW
    default_search_engine_overridden, -- NEW
    default_private_search_engine_display_name AS default_private_search_engine,
    default_private_search_engine_load_path AS default_private_search_engine_data_load_path,
    default_private_search_engine_submission_url AS default_private_search_engine_data_submission_url,
    default_private_search_engine_partner_code, -- NEW
    default_private_search_engine_provider_id, -- NEW
    default_private_search_engine_overridden, -- NEW
    sample_id,
    ping_start_time, -- NEW
    ping_end_time, -- NEW
    ping_seq, -- NEW
    overridden_by_third_party, -- NEW
    max_concurrent_tab_count_max,
    experiments,
    profile_age_in_days,
    serp_searches_organic_count AS organic,
    serp_searches_tagged_count AS tagged_serp,
    serp_follow_on_searches_tagged_count AS tagged_follow_on,
    sap_counts_total,
    serp_counts_total,
    serp_ad_click_target AS ad_click_target,
    serp_num_ad_clicks AS ad_click_total,
    serp_ad_clicks_tagged_count AS ad_click_tagged,
    serp_ad_clicks_organic_count AS ad_click_organic,
    serp_with_ads_tagged_count AS search_with_ads_tagged,
    serp_with_ads_organic_count AS search_with_ads_organic,
    serp_ad_blocker_inferred AS ad_blocker_inferred,
    serp_num_non_ad_link_clicks AS num_non_ad_link_clicks, -- NEW
    serp_num_other_engagements AS num_other_engagements, -- NEW
    serp_num_ads_loaded AS num_ads_loaded, -- NEW
    serp_num_ads_visible AS num_ads_visible, -- NEW
    serp_num_ads_blocked AS num_ads_blocked, -- NEW
    serp_num_ads_notshowing AS num_ads_notshowing, -- NEW
    has_adblocker_addon,
    policies_is_enterprise,
    -- keep these after the coalesce, so they read the same os, os_version and
    -- windows_build_number the row publishes. NULL where that os_version does not parse.
    -- major and minor are deliberately identical on Windows: both are the release name.
    CASE
      WHEN mozfun.norm.os(os) = "Windows"
        THEN mozfun.norm.windows_version_info(os, os_version, windows_build_number)
      ELSE CAST(mozfun.norm.truncate_version(os_version, "major") AS STRING)
    END AS os_version_major,
    CASE
      WHEN mozfun.norm.os(os) = "Windows"
        THEN mozfun.norm.windows_version_info(os, os_version, windows_build_number)
      ELSE CAST(mozfun.norm.truncate_version(os_version, "minor") AS STRING)
    END AS os_version_minor,
    profile_group_id,
    sap_provider_id, -- NEW
    sap_provider_name -- NEW
  FROM
    join_sap_serp_cte
)
SELECT
  *
FROM
  final_cte
