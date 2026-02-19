-- =============================================================================
-- CTE 1: TOF (Top of Funnel) - Web Analytics from GA4
-- =============================================================================
-- Source: GA4 sessions from mozilla.org and firefox.com
-- Metrics: Visits (sessions) and Downloads (click events)
-- Granularity: Daily, by country, funnel type, and attribution dimensions
-- =============================================================================
WITH top_of_funnel_base AS (
  SELECT
    session_date AS submission_date,
    country_names.code AS country_code,
    LOWER(os) AS os,
    -- Attribution dimensions for channel/campaign analysis
    ad_crosschannel_primary_channel_group AS channel_raw,
    ad_google_campaign_id AS campaign_id,
    COALESCE(ad_google_campaign, campaigns_v2.campaign_name) AS campaign,
    ad_crosschannel_source AS `source`,
    ad_crosschannel_medium AS medium,
    -- Logic: Exclude sessions where mozilla.org referred to firefox.com or vice versa
    -- This prevents double-counting users who navigate between our properties
    COALESCE(
      (
        -- Exclude: mozilla.org traffic landing on firefox.com
        (
          (
            REGEXP_CONTAINS(manual_source, r'(?i)^(.*www.)?mozilla.org.*')
            OR REGEXP_CONTAINS(first_source_from_event_params, r'(?i)^(.*www.)?mozilla.org.*')
          )
          AND flag = 'FIREFOX.COM'
        )
        -- Exclude: firefox.com traffic landing on mozilla.org
        OR (
          (
            LOWER(manual_source) IN ('www.firefox.com', 'firefox.com', 'firefox-com')
            OR LOWER(first_source_from_event_params) IN (
              'www.firefox.com',
              'firefox.com',
              'firefox-com'
            )
          )
          AND flag = 'MOZILLA.ORG'
        )
      ),
      FALSE
    ) IS FALSE AS has_visits,
    firefox_desktop_downloads,
  FROM
    `moz-fx-data-shared-prod.telemetry.ga4_sessions_firefoxcom_mozillaorg_combined` AS ga4
  LEFT JOIN
    `moz-fx-data-shared-prod.google_ads_derived.campaigns_v2` AS campaigns_v2
    ON SAFE_CAST(ga4.first_gad_campaignid_from_event_params AS INT64) = campaigns_v2.campaign_id
  LEFT JOIN
    `moz-fx-data-shared-prod.static.country_names_v1` AS country_names
    ON ga4.country = country_names.name
  WHERE
    session_date = DATE_SUB(@submission_date, INTERVAL 27 day)
    AND device_category = 'desktop'
    -- Exclude existing Firefox users - we want new acquisition only
    AND COALESCE(browser, '') NOT IN ('Firefox', 'Mozilla')
),
top_of_funnel AS (
  SELECT
    top_of_funnel_base.* EXCEPT (os, country_code, has_visits, firefox_desktop_downloads),
    -- Country format: "US (Tier 1)" or "ROW" for Rest of World (Tier 3)
    IF(
      COALESCE(tier_mapping.tier, 'Tier 3') = 'Tier 3',
      "ROW",
      tier_mapping.country_code || " (" || tier_mapping.tier || ")"
    ) AS country,
    -- Determine funnel type based on OS
    CASE
      WHEN os = 'windows'
        THEN 'mozorg windows funnel'
      WHEN os = 'macintosh'
        THEN 'mozorg mac funnel'
      ELSE 'mozorg other'
    END AS funnel_derived,
    -- VISITS: Count sessions, excluding internal cross-site traffic
    COUNTIF(has_visits) AS visits,
    -- DOWNLOADS: Count sessions with at least one Firefox desktop download
    COUNTIF(firefox_desktop_downloads > 0) AS downloads
  FROM
    top_of_funnel_base
  LEFT JOIN
    `moz-fx-data-shared-prod.static.marketing_country_tier_mapping_v1` AS tier_mapping
    USING (country_code)
  WHERE
    NOT COALESCE(tier_mapping.has_web_cookie_consent, FALSE)
  GROUP BY
    ALL
),
-- =============================================================================
-- CTE 2: ga4_attr_by_dltoken - Deduplicated Attribution by Download Token
-- =============================================================================
-- PURPOSE: Get one attribution record per download token (dltoken)
-- PROBLEM: One dltoken can map to multiple client_ids in cfs_ga4_attr
--          (e.g., user creates multiple Firefox profiles from same download)
-- SOLUTION: Pick the "best" record per dltoken using priority:
--   1. Records with channel data (ga4_ad_crosschannel_primary_channel_group IS NOT NULL)
--   2. Earliest first_seen_date
--   3. client_id as tiebreaker
-- USED BY: installs CTE for attribution lookup
-- =============================================================================
-- deduplicated attribution data (prevents many-to-many explosion)
ga4_attribution AS (
  SELECT
    ga4_attr.client_id,
    ga4_attr.first_seen_date,
    ga4_attr.ga4_session_date,
    ga4_attr.attribution_dltoken AS dltoken,
    ga4_attr.ga4_ad_crosschannel_primary_channel_group AS channel_raw,
    ga4_attr.ga4_ad_google_campaign,
    ga4_attr.ga4_ad_google_campaign_id AS campaign_id,
    ga4_attr.ga4_ad_crosschannel_source AS `source`,
    ga4_attr.ga4_ad_crosschannel_medium AS medium,
    ga4_attr.ga4_first_gad_campaignid_from_event_params,
    COALESCE(ga4_attr.ga4_ad_google_campaign, campaigns_v2.campaign_name) AS campaign,
  FROM
    `moz-fx-data-shared-prod.telemetry.cfs_ga4_attr` AS ga4_attr
  LEFT JOIN
    `moz-fx-data-shared-prod.google_ads_derived.campaigns_v2` AS campaigns_v2
    ON SAFE_CAST(ga4_first_gad_campaignid_from_event_params AS INT64) = campaigns_v2.campaign_id
  WHERE
    attribution_dltoken IS NOT NULL
),
ga4_attr_by_dltoken_dedup AS (
  SELECT
    client_id,
    first_seen_date,
    ga4_session_date,
    dltoken,
    channel_raw,
    `source`,
    medium,
    ga4_first_gad_campaignid_from_event_params,
    campaign_id,
    campaign,
  FROM
    ga4_attribution
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        dltoken
      ORDER BY
        CASE
          WHEN channel_raw IS NOT NULL
            THEN 0
          ELSE 1
        END,
        first_seen_date,
        client_id
    ) = 1
),
-- =============================================================================
-- CTE 3: installs - Windows Installer Telemetry
-- =============================================================================
-- Source: Firefox installer ping (Windows only - Mac has no installer telemetry)
-- Metrics: Successful new installs (not upgrades/reinstalls)
-- Attribution: Joined to ga4_attr_by_dltoken via dltoken extracted from attribution URL
-- Note: ~10% of dltokens map to multiple profiles, hence the deduplication above
-- =============================================================================
windows_installer_installs_base AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_country_code AS country_code,
  -- Extract dltoken from URL-encoded attribution string
    REGEXP_EXTRACT(attribution, r'dltoken%3D([^%&]+)') AS dltoken,
  FROM
    `moz-fx-data-shared-prod.firefox_installer.install` AS installs
  WHERE
    DATE(submission_timestamp) = DATE_SUB(@submission_date, INTERVAL 27 day)
    AND succeeded  -- Only successful installs
    AND NOT had_old_install  -- Exclude upgrades/reinstalls (new installs only)
  -- Only count installs from tracked mozilla.org downloads
    AND funnel_derived = 'mozorg windows funnel'
    AND build_channel = 'release'
),
windows_installer_installs AS (
  SELECT
    submission_date,
    -- Country format: "US (Tier 1)" or "ROW" for Rest of World (Tier 3)
    IF(
      COALESCE(tier_mapping.tier, 'Tier 3') = 'Tier 3',
      "ROW",
      tier_mapping.country_code || " (" || tier_mapping.tier || ")"
    ) AS country,
    'mozorg windows funnel' AS funnel_derived,  -- Always Windows (Mac has no installer data)
    -- Get attribution from ga4_attr_by_dltoken lookup
    channel_raw,
    campaign,
    campaign_id,
    `source`,
    medium,
    COUNT(*) AS installs,
  FROM
    windows_installer_installs_base
  LEFT JOIN
    `moz-fx-data-shared-prod.static.marketing_country_tier_mapping_v1` AS tier_mapping
    USING (country_code)
  LEFT JOIN
    ga4_attr_by_dltoken_dedup
    USING (dltoken)
  WHERE
    NOT COALESCE(tier_mapping.has_web_cookie_consent, FALSE)
  GROUP BY
    ALL
),
-- =============================================================================
-- CTE 4: fresh_download_profiles - Profile Filtering (Currently Unused)
-- =============================================================================
-- PURPOSE: Identify profiles that were created shortly after a tracked download
-- LOGIC: Only count the FIRST profile per download token within 30 days
-- USE CASE: Could be used to filter desktop_funnels_telemetry for stricter attribution
-- STATUS: Currently not used in final join but kept for potential future use
-- =============================================================================
fresh_download_profiles AS (
  SELECT
    cfs.client_id,
    cfs.first_seen_date,
    cfs.attribution_dltoken,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_first_seen_28_days_later` AS cfs
  INNER JOIN
    ga4_attr_by_dltoken_dedup AS ga4_attr
    USING (client_id)
  WHERE
    cfs.funnel_derived IN ('mozorg windows funnel', 'mozorg mac funnel')
    AND cfs.first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 day)
    AND cfs.is_desktop
    AND cfs.attribution_dltoken IS NOT NULL
  -- Download must have occurred within 30 days before profile creation
    AND ga4_attr.ga4_session_date
    BETWEEN DATE_SUB(cfs.first_seen_date, INTERVAL 30 DAY)
    AND cfs.first_seen_date
  -- Assign row id to profiles per download to identify the first one
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        cfs.attribution_dltoken
      ORDER BY
        cfs.first_seen_date,
        cfs.client_id
    ) = 1
),
-- =============================================================================
-- CTE 5: desktop_funnels_telemetry - Firefox Profile Metrics
-- =============================================================================
-- Source: Firefox telemetry (clients_first_seen_28_days_later)
-- Metrics:
--   - new_profiles: Count of new Firefox profiles created
--   - return_user: Profiles that were active on day 2 (qualified_second_day)
--   - retained_week4: Profiles still active in week 4 (qualified_week4)
-- Granularity: Daily, by country, funnel type, and attribution dimensions
-- =============================================================================
desktop_funnels_telemetry AS (
  SELECT
    cfs28.first_seen_date AS submission_date,
    IF(
      COALESCE(tier_mapping.tier, 'Tier 3') = 'Tier 3',
      "ROW",
      tier_mapping.country_code || " (" || tier_mapping.tier || ")"
    ) AS country,
    -- Reclassify "other" and EU countries as "Unknown" funnel
    IF(
      cfs28.funnel_derived = 'other'
      OR tier_mapping.has_web_cookie_consent,
      'Unknown',
      cfs28.funnel_derived
    ) AS funnel_derived,
    cfs28.normalized_os,
    -- Partner-specific dimensions (NULL for non-partner funnels to reduce cardinality)
    IF(cfs28.funnel_derived = 'partner', cfs28.partner_org, CAST(NULL AS STRING)) AS partner_org,
    IF(
      cfs28.funnel_derived = 'partner',
      cfs28.distribution_model,
      CAST(NULL AS STRING)
    ) AS distribution_model,
    IF(
      cfs28.funnel_derived = 'partner',
      cfs28.distribution_id,
      CAST(NULL AS STRING)
    ) AS distribution_id,
    -- Attribution dimensions from GA4
    ga4_attr.channel_raw,
    ga4_attr.campaign,
    ga4_attr.campaign_id,
    ga4_attr.`source`,
    ga4_attr.medium,
    -- Profile metrics
    COUNT(*) AS new_profiles,
    COUNTIF(qualified_second_day) AS return_user,      -- Active on day 2
    COUNTIF(qualified_week4) AS retained_week4         -- Active in week 4
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_first_seen_28_days_later` AS cfs28
  -- Join to GA4 attribution directly (no dltoken dedup here as multiple client_ids could come from the same dltoken)
  LEFT JOIN
    ga4_attribution AS ga4_attr
    USING (client_id)
  LEFT JOIN
    `moz-fx-data-shared-prod.static.marketing_country_tier_mapping_v1` AS tier_mapping
    ON cfs28.country = tier_mapping.country_code
  WHERE
    cfs28.first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 day)
    AND cfs28.is_desktop
    AND cfs28.normalized_channel = 'release'
    -- Exclude EU for core funnels, but keep partner funnel metrics
    AND (
      NOT COALESCE(tier_mapping.has_web_cookie_consent, FALSE)
      OR cfs28.funnel_derived = 'partner'
    )
  GROUP BY
    ALL
),
-- =============================================================================
-- CTE 6: final - Unified Funnel Metrics
-- =============================================================================
-- Combines TOF (visits/downloads), installs, and telemetry (profiles/retention)
-- Join strategy: FULL JOINs to preserve all data even when metrics don't align
-- Attribution dimensions must match for rows to combine
-- =============================================================================
final AS (
  SELECT
    -- Use COALESCE to get dimensions from whichever source has data (TOF, installs, or telemetry)
    COALESCE(
      top_of_funnel.submission_date,
      windows_installer.submission_date,
      desktop_funnels_telemetry.submission_date
    ) AS funnel_date,
    COALESCE(
      top_of_funnel.country,
      windows_installer.country,
      desktop_funnels_telemetry.country
    ) AS country,
    COALESCE(
      top_of_funnel.funnel_derived,
      windows_installer.funnel_derived,
      desktop_funnels_telemetry.funnel_derived
    ) AS funnel_derived,
    COALESCE(
      top_of_funnel.channel_raw,
      windows_installer.channel_raw,
      desktop_funnels_telemetry.channel_raw
    ) AS channel_raw,
    COALESCE(
      top_of_funnel.campaign,
      windows_installer.campaign,
      desktop_funnels_telemetry.campaign
    ) AS campaign,
    COALESCE(
      top_of_funnel.campaign_id,
      windows_installer.campaign_id,
      desktop_funnels_telemetry.campaign_id
    ) AS campaign_id,
    COALESCE(
      top_of_funnel.`source`,
      windows_installer.`source`,
      desktop_funnels_telemetry.`source`
    ) AS `source`,
    COALESCE(
      top_of_funnel.medium,
      windows_installer.medium,
      desktop_funnels_telemetry.medium
    ) AS medium,
    desktop_funnels_telemetry.normalized_os,
    desktop_funnels_telemetry.partner_org,
    desktop_funnels_telemetry.distribution_model,
    NULLIF(desktop_funnels_telemetry.distribution_id, "") AS distribution_id,
    -- Partner funnel has no web visits/downloads (starts at profiles)
    IF(
      desktop_funnels_telemetry.funnel_derived = 'partner',
      CAST(NULL AS INTEGER),
      COALESCE(top_of_funnel.visits, 0)
    ) AS visits,
    IF(
      desktop_funnels_telemetry.funnel_derived = 'partner',
      CAST(NULL AS INTEGER),
      COALESCE(top_of_funnel.downloads, 0)
    ) AS downloads,
    -- Installs only available for Windows funnel (Mac has no installer telemetry)
    CASE
      WHEN COALESCE(
          top_of_funnel.funnel_derived,
          desktop_funnels_telemetry.funnel_derived
        ) = 'mozorg windows funnel'
        THEN COALESCE(windows_installer.installs, 0)
      WHEN COALESCE(
          top_of_funnel.funnel_derived,
          desktop_funnels_telemetry.funnel_derived
        ) = 'mozorg mac funnel'
        THEN CAST(NULL AS INTEGER)
      ELSE CAST(NULL AS INTEGER)
    END AS installs,
    COALESCE(desktop_funnels_telemetry.new_profiles, 0) AS new_profiles,
    COALESCE(desktop_funnels_telemetry.return_user, 0) AS return_user,
    COALESCE(desktop_funnels_telemetry.retained_week4, 0) AS retained_week4
  FROM
    top_of_funnel
  -- Join installs on all attribution dimensions
  FULL JOIN
    windows_installer_installs AS windows_installer
    ON top_of_funnel.submission_date = windows_installer.submission_date
    AND top_of_funnel.country = windows_installer.country
    AND top_of_funnel.funnel_derived = windows_installer.funnel_derived
    AND COALESCE(top_of_funnel.channel_raw, '') = COALESCE(windows_installer.channel_raw, '')
    AND COALESCE(top_of_funnel.campaign, '') = COALESCE(windows_installer.campaign, '')
    AND COALESCE(top_of_funnel.campaign_id, '') = COALESCE(windows_installer.campaign_id, '')
    AND COALESCE(top_of_funnel.source, '') = COALESCE(windows_installer.source, '')
    AND COALESCE(top_of_funnel.medium, '') = COALESCE(windows_installer.medium, '')
  -- Join telemetry (profiles) on all attribution dimensions
  FULL JOIN
    desktop_funnels_telemetry
    ON top_of_funnel.submission_date = desktop_funnels_telemetry.submission_date
    AND top_of_funnel.country = desktop_funnels_telemetry.country
    AND top_of_funnel.funnel_derived = desktop_funnels_telemetry.funnel_derived
    AND COALESCE(top_of_funnel.channel_raw, '') = COALESCE(
      desktop_funnels_telemetry.channel_raw,
      ''
    )
    AND COALESCE(top_of_funnel.campaign, '') = COALESCE(desktop_funnels_telemetry.campaign, '')
    AND COALESCE(top_of_funnel.campaign_id, '') = COALESCE(
      desktop_funnels_telemetry.campaign_id,
      ''
    )
    AND COALESCE(top_of_funnel.source, '') = COALESCE(desktop_funnels_telemetry.source, '')
    AND COALESCE(top_of_funnel.medium, '') = COALESCE(desktop_funnels_telemetry.medium, '')
)
-- =============================================================================
-- Final SELECT: Add Derived Dimensions and Year-over-Year Metrics
-- =============================================================================
SELECT
  * REPLACE (
    -- Remap tier names to user-friendly labels: Tier 1 -> Core, Tier 2 -> Growth, ROW -> Emerging
    CASE
      WHEN country = 'ROW'
        THEN 'Emerging'
      ELSE REGEXP_REPLACE(REGEXP_REPLACE(country, r'Tier 1', 'Core'), r'Tier 2', 'Growth')
    END AS country
  ),
  -- Country tier as separate dimension for filtering
  CASE
    WHEN country = 'ROW'
      THEN 'Emerging'
    WHEN country LIKE '%(Tier 1)'
      THEN 'Core'
    WHEN country LIKE '%(Tier 2)'
      THEN 'Growth'
    ELSE 'Unknown'
  END AS country_tier,
  -- Derived channel: simplify attribution into three buckets
  --   - Paid Search: Has a campaign (from Google Ads)
  --   - Organic Search: GA4 classified as Organic Search
  --   - Web - Organic/Other: Everything else (direct, referral, social, etc.)
  CASE
    WHEN campaign IS NOT NULL
      THEN 'Paid Search'
    WHEN channel_raw = 'Organic Search'
      THEN 'Organic Search'
    ELSE 'Web - Organic/Other'
  END AS channel,
  -- Subchannel: Campaign type classification for Paid Search
  -- Based on naming conventions: _brand_, _nonbrand_, _comp_, pmax
  CASE
    WHEN campaign IS NOT NULL
      THEN
        CASE
          WHEN REGEXP_CONTAINS(campaign, r'_brand_')
            THEN 'Brand'
          WHEN REGEXP_CONTAINS(campaign, r'_nonbrand_')
            THEN 'Non-Brand'
          WHEN REGEXP_CONTAINS(campaign, r'_comp_')
            THEN 'Competitor'
          WHEN REGEXP_CONTAINS(LOWER(campaign), r'pmax')
            THEN 'PMax'
          ELSE 'Other'
        END
    ELSE 'All'  -- Non-Paid Search has no subchannel
  END AS subchannel,
FROM
  final
