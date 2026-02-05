-- =============================================================================
-- CTE 1: TOF (Top of Funnel) - Web Analytics from GA4
-- =============================================================================
-- Source: GA4 sessions from mozilla.org and firefox.com
-- Metrics: Visits (sessions) and Downloads (click events)
-- Granularity: Daily, by country, funnel type, and attribution dimensions
-- =============================================================================
WITH TOF AS (
  SELECT
    session_date AS submission_date,
    -- Country format: "US (Tier 1)" or "ROW" for Rest of World (Tier 3)
    CASE
      WHEN COALESCE(tier_mapping.tier, 'Tier 3') = 'Tier 3'
        THEN 'ROW'
      ELSE tier_mapping.country_code || " (" || tier_mapping.tier || ")"
    END AS country,
    -- Determine funnel type based on OS
    CASE
      WHEN LOWER(os) = 'windows'
        THEN 'mozorg windows funnel'
      WHEN LOWER(os) = 'macintosh'
        THEN 'mozorg mac funnel'
      ELSE 'mozorg other'
    END AS funnel_derived,
    -- Attribution dimensions for channel/campaign analysis
    ad_crosschannel_primary_channel_group AS channel_raw,
    IFNULL(ad_google_campaign, campaigns_v2.campaign_name) AS campaign,
    ad_google_campaign_id AS campaign_id,
    ad_crosschannel_source AS source,
    ad_crosschannel_medium AS medium,
    -- VISITS: Count sessions, excluding internal cross-site traffic
    -- Logic: Exclude sessions where mozilla.org referred to firefox.com or vice versa
    -- This prevents double-counting users who navigate between our properties
    COUNTIF(
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
      ) IS FALSE
    ) AS visits,
    -- DOWNLOADS: Count sessions with at least one Firefox desktop download
    COUNTIF(firefox_desktop_downloads > 0) AS downloads
  FROM
    `mozdata.telemetry.ga4_sessions_firefoxcom_mozillaorg_combined` ga4
  LEFT JOIN
    `mozdata.analysis.marketing_country_tier_mapping` AS tier_mapping
    -- TODO: Should use country normalization table for cleaner mapping
    ON
    CASE
      WHEN ga4.country = 'Türkiye'
        THEN 'Turkey'
      ELSE ga4.country
    END
    =tier_mapping.country
  LEFT JOIN
    `moz-fx-data-shared-prod.google_ads_derived.campaigns_v2` campaigns_v2
    ON SAFE_CAST(ga4.first_gad_campaignid_from_event_params AS INT64) = campaigns_v2.campaign_id
  WHERE
    session_date >= '2021-01-01'
    AND device_category = 'desktop'
    -- Exclude existing Firefox users - we want new acquisition only
    AND COALESCE(browser, '') NOT IN ('Firefox', 'Mozilla')
    -- Exclude EU due to cookie consent banner reducing tracking coverage
    AND NOT COALESCE(tier_mapping.is_eu, FALSE)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
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
ga4_attr_by_dltoken AS (
  SELECT
    sub.attribution_dltoken AS dltoken,
    sub.ga4_ad_crosschannel_primary_channel_group AS channel_raw,
    IFNULL(sub.ga4_ad_google_campaign, campaigns_v2.campaign_name) AS campaign,
    sub.ga4_ad_google_campaign_id AS campaign_id,
    sub.ga4_ad_crosschannel_source AS source,
    sub.ga4_ad_crosschannel_medium AS medium
  FROM
    (
      SELECT
        attribution_dltoken,
        ga4_ad_crosschannel_primary_channel_group,
        ga4_ad_google_campaign,
        ga4_ad_google_campaign_id,
        ga4_ad_crosschannel_source,
        ga4_ad_crosschannel_medium,
        ga4_first_gad_campaignid_from_event_params,
        first_seen_date,
        client_id,
      -- Rank records per dltoken, preferring those with attribution data
        ROW_NUMBER() OVER (
          PARTITION BY
            attribution_dltoken
          ORDER BY
            CASE
              WHEN ga4_ad_crosschannel_primary_channel_group IS NOT NULL
                THEN 0
              ELSE 1
            END,
            first_seen_date,
            client_id
        ) AS rn
      FROM
        `moz-fx-data-shared-prod.telemetry.cfs_ga4_attr`
      WHERE
        attribution_dltoken IS NOT NULL
    ) sub
  LEFT JOIN
    `moz-fx-data-shared-prod.google_ads_derived.campaigns_v2` campaigns_v2
    ON SAFE_CAST(sub.ga4_first_gad_campaignid_from_event_params AS INT64) = campaigns_v2.campaign_id
  -- Keep only the best record per dltoken
  WHERE
    rn = 1
),
-- =============================================================================
-- CTE 3: installs - Windows Installer Telemetry
-- =============================================================================
-- Source: Firefox installer ping (Windows only - Mac has no installer telemetry)
-- Metrics: Successful new installs (not upgrades/reinstalls)
-- Attribution: Joined to ga4_attr_by_dltoken via dltoken extracted from attribution URL
-- Note: ~10% of dltokens map to multiple profiles, hence the deduplication above
-- =============================================================================
installs AS (
  SELECT
    DATE(i.submission_timestamp) AS submission_date,
    CASE
      WHEN COALESCE(tier_mapping.tier, 'Tier 3') = 'Tier 3'
        THEN 'ROW'
      ELSE tier_mapping.country_code || " (" || tier_mapping.tier || ")"
    END AS country,
    'mozorg windows funnel' AS funnel_derived,  -- Always Windows (Mac has no installer data)
    -- Get attribution from deduplicated ga4_attr lookup
    ga4_attr.channel_raw,
    ga4_attr.campaign,
    ga4_attr.campaign_id,
    ga4_attr.source,
    ga4_attr.medium,
    COUNT(*) AS installs
  FROM
    (
    -- Subquery to extract dltoken and filter install records
      SELECT
        submission_timestamp,
        normalized_country_code,
      -- Extract dltoken from URL-encoded attribution string
        REGEXP_EXTRACT(attribution, r'dltoken%3D([^%&]+)') AS dltoken
      FROM
        `mozdata.firefox_installer.install`
      WHERE
        DATE(submission_timestamp) >= '2021-01-01'
        AND succeeded = TRUE              -- Only successful installs
        AND had_old_install IS NOT TRUE   -- Exclude upgrades/reinstalls (new installs only)
        AND DATE_DIFF(CURRENT_DATE(), DATE(submission_timestamp), DAY) > 1  -- Data maturity lag
      -- Only count installs from tracked mozilla.org downloads
        AND funnel_derived = 'mozorg windows funnel'
    ) i
  LEFT JOIN
    `mozdata.analysis.marketing_country_tier_mapping` AS tier_mapping
    ON i.normalized_country_code = tier_mapping.country_code
  -- Join to deduplicated attribution data (prevents many-to-many explosion)
  LEFT JOIN
    ga4_attr_by_dltoken ga4_attr
    ON i.dltoken = ga4_attr.dltoken
  WHERE
    NOT COALESCE(tier_mapping.is_eu, FALSE)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
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
    client_id,
    first_seen_date
  FROM
    (
      SELECT
        cfs.client_id,
        cfs.first_seen_date,
        cfs.attribution_dltoken,
      -- Rank profiles per download to identify the first one
        ROW_NUMBER() OVER (
          PARTITION BY
            cfs.attribution_dltoken
          ORDER BY
            cfs.first_seen_date,
            cfs.client_id
        ) AS profile_rank
      FROM
        `mozdata.telemetry.clients_first_seen_28_days_later` cfs
      INNER JOIN
        `moz-fx-data-shared-prod.telemetry.cfs_ga4_attr` ga4_attr
        ON cfs.client_id = ga4_attr.client_id
      WHERE
        cfs.funnel_derived IN ('mozorg windows funnel', 'mozorg mac funnel')
        AND cfs.first_seen_date >= '2021-01-01'
        AND cfs.is_desktop = TRUE
        AND cfs.attribution_dltoken IS NOT NULL
      -- Download must have occurred within 30 days before profile creation
        AND ga4_attr.ga4_session_date
        BETWEEN DATE_SUB(cfs.first_seen_date, INTERVAL 30 DAY)
        AND cfs.first_seen_date
    )
  -- Only keep the first profile created from each download
  WHERE
    profile_rank = 1
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
    CASE
      WHEN COALESCE(tier_mapping.tier, 'Tier 3') = 'Tier 3'
        THEN 'ROW'
      ELSE tier_mapping.country_code || " (" || tier_mapping.tier || ")"
    END AS country,
    -- Reclassify "other" and EU countries as "Unknown" funnel
    CASE
      WHEN cfs28.funnel_derived = 'other'
        OR tier_mapping.is_eu
        THEN 'Unknown'
      ELSE cfs28.funnel_derived
    END AS funnel_derived,
    cfs28.normalized_os,
    -- Partner-specific dimensions (NULL for non-partner funnels to reduce cardinality)
    CASE
      WHEN cfs28.funnel_derived = 'partner'
        THEN cfs28.partner_org
    END AS partner_org,
    CASE
      WHEN cfs28.funnel_derived = 'partner'
        THEN cfs28.distribution_model
    END AS distribution_model,
    CASE
      WHEN cfs28.funnel_derived = 'partner'
        THEN cfs28.distribution_id
    END AS distribution_id,
    -- Attribution dimensions from GA4
    ga4_attr.ga4_ad_crosschannel_primary_channel_group AS channel_raw,
    IFNULL(ga4_attr.ga4_ad_google_campaign, campaigns_v2.campaign_name) AS campaign,
    ga4_attr.ga4_ad_google_campaign_id AS campaign_id,
    ga4_attr.ga4_ad_crosschannel_source AS source,
    ga4_attr.ga4_ad_crosschannel_medium AS medium,
    -- Profile metrics
    COUNT(*) AS new_profiles,
    COUNTIF(qualified_second_day) AS return_user,      -- Active on day 2
    COUNTIF(qualified_week4) AS retained_week4         -- Active in week 4
  FROM
    `mozdata.telemetry.clients_first_seen_28_days_later` cfs28
  LEFT JOIN
    `mozdata.analysis.marketing_country_tier_mapping` AS tier_mapping
    ON cfs28.country = tier_mapping.country_code
  -- Join to GA4 attribution (1:1 via client_id, no deduplication needed here)
  LEFT JOIN
    `moz-fx-data-shared-prod.telemetry.cfs_ga4_attr` ga4_attr
    ON cfs28.client_id = ga4_attr.client_id
  LEFT JOIN
    `moz-fx-data-shared-prod.google_ads_derived.campaigns_v2` campaigns_v2
    ON SAFE_CAST(
      ga4_attr.ga4_first_gad_campaignid_from_event_params AS INT64
    ) = campaigns_v2.campaign_id
  WHERE
    cfs28.first_seen_date >= '2021-01-01'
    AND cfs28.is_desktop = TRUE
    -- Exclude EU for core funnels, but keep partner funnel metrics
    AND (NOT COALESCE(tier_mapping.is_eu, FALSE) OR cfs28.funnel_derived = 'partner')
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12
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
    COALESCE(t.submission_date, i.submission_date, d.submission_date) AS submission_date,
    COALESCE(t.country, i.country, d.country) AS country,
    COALESCE(t.funnel_derived, i.funnel_derived, d.funnel_derived) AS funnel_derived,
    COALESCE(t.channel_raw, i.channel_raw, d.channel_raw) AS channel_raw,
    COALESCE(t.campaign, i.campaign, d.campaign) AS campaign,
    COALESCE(t.campaign_id, i.campaign_id, d.campaign_id) AS campaign_id,
    COALESCE(t.source, i.source, d.source) AS source,
    COALESCE(t.medium, i.medium, d.medium) AS medium,
    d.normalized_os,
    d.partner_org,
    d.distribution_model,
    d.distribution_id,
    -- Partner funnel has no web visits/downloads (starts at profiles)
    CASE
      WHEN d.funnel_derived = 'partner'
        THEN NULL
      ELSE COALESCE(t.visits, 0)
    END AS visits,
    CASE
      WHEN d.funnel_derived = 'partner'
        THEN NULL
      ELSE COALESCE(t.downloads, 0)
    END AS downloads,
    -- Installs only available for Windows funnel (Mac has no installer telemetry)
    CASE
      WHEN COALESCE(t.funnel_derived, d.funnel_derived) = 'mozorg windows funnel'
        THEN COALESCE(i.installs, 0)
      WHEN COALESCE(t.funnel_derived, d.funnel_derived) = 'mozorg mac funnel'
        THEN NULL
      ELSE NULL
    END AS installs,
    COALESCE(d.new_profiles, 0) AS new_profiles,
    COALESCE(d.return_user, 0) AS return_user,
    COALESCE(d.retained_week4, 0) AS retained_week4
  FROM
    TOF t
  -- Join installs on all attribution dimensions
  FULL JOIN
    installs i
    ON t.submission_date = i.submission_date
    AND t.country = i.country
    AND t.funnel_derived = i.funnel_derived
    AND COALESCE(t.channel_raw, '') = COALESCE(i.channel_raw, '')
    AND COALESCE(t.campaign, '') = COALESCE(i.campaign, '')
    AND COALESCE(t.campaign_id, '') = COALESCE(i.campaign_id, '')
    AND COALESCE(t.source, '') = COALESCE(i.source, '')
    AND COALESCE(t.medium, '') = COALESCE(i.medium, '')
  -- Join telemetry (profiles) on all attribution dimensions
  FULL JOIN
    desktop_funnels_telemetry d
    ON t.submission_date = d.submission_date
    AND t.country = d.country
    AND t.funnel_derived = d.funnel_derived
    AND COALESCE(t.channel_raw, '') = COALESCE(d.channel_raw, '')
    AND COALESCE(t.campaign, '') = COALESCE(d.campaign, '')
    AND COALESCE(t.campaign_id, '') = COALESCE(d.campaign_id, '')
    AND COALESCE(t.source, '') = COALESCE(d.source, '')
    AND COALESCE(t.medium, '') = COALESCE(d.medium, '')
  -- Only include dates where telemetry data has matured (28-day metrics need time)
  WHERE
    COALESCE(t.submission_date, d.submission_date) <= (
      SELECT
        MAX(submission_date)
      FROM
        desktop_funnels_telemetry
    )
)
-- =============================================================================
-- Final SELECT: Add Derived Dimensions and Year-over-Year Metrics
-- =============================================================================
SELECT
  f.* REPLACE (
    -- Remap tier names to user-friendly labels: Tier 1 -> Core, Tier 2 -> Growth, ROW -> Emerging
    CASE
      WHEN f.country = 'ROW'
        THEN 'Emerging'
      ELSE REGEXP_REPLACE(REGEXP_REPLACE(f.country, r'Tier 1', 'Core'), r'Tier 2', 'Growth')
    END AS country
  ),
  -- Country tier as separate dimension for filtering
  CASE
    WHEN f.country = 'ROW'
      THEN 'Emerging'
    WHEN f.country LIKE '%(Tier 1)'
      THEN 'Core'
    WHEN f.country LIKE '%(Tier 2)'
      THEN 'Growth'
    ELSE 'Unknown'
  END AS country_tier,
  -- Derived channel: simplify attribution into three buckets
  --   - Paid Search: Has a campaign (from Google Ads)
  --   - Organic Search: GA4 classified as Organic Search
  --   - Web - Organic/Other: Everything else (direct, referral, social, etc.)
  CASE
    WHEN f.campaign IS NOT NULL
      THEN 'Paid Search'
    WHEN f.channel_raw = 'Organic Search'
      THEN 'Organic Search'
    ELSE 'Web - Organic/Other'
  END AS channel,
  -- Subchannel: Campaign type classification for Paid Search
  -- Based on naming conventions: _brand_, _nonbrand_, _comp_, pmax
  CASE
    WHEN f.campaign IS NOT NULL
      THEN
        CASE
          WHEN REGEXP_CONTAINS(f.campaign, r'_brand_')
            THEN 'Brand'
          WHEN REGEXP_CONTAINS(f.campaign, r'_nonbrand_')
            THEN 'Non-Brand'
          WHEN REGEXP_CONTAINS(f.campaign, r'_comp_')
            THEN 'Competitor'
          WHEN REGEXP_CONTAINS(LOWER(f.campaign), r'pmax')
            THEN 'PMax'
          ELSE 'Other'
        END
    ELSE 'All'  -- Non-Paid Search has no subchannel
  END AS subchannel,
  -- Year-over-year metrics (364 days to align day-of-week)
  COALESCE(ly.visits, 0) AS visits_ly,
  COALESCE(ly.downloads, 0) AS downloads_ly,
  COALESCE(ly.installs, 0) AS installs_ly,
  COALESCE(ly.new_profiles, 0) AS new_profiles_ly,
  COALESCE(ly.return_user, 0) AS return_user_ly,
  COALESCE(ly.retained_week4, 0) AS retained_week4_ly
FROM
  final f
-- Self-join to get last year's metrics for YoY comparison
-- Using 364 days (52 weeks) to align day-of-week for fair comparison
LEFT JOIN
  final ly
  ON f.submission_date = ly.submission_date + INTERVAL 364 DAY
  AND f.country = ly.country
  AND f.funnel_derived = ly.funnel_derived
  AND COALESCE(f.channel_raw, '') = COALESCE(ly.channel_raw, '')
  AND COALESCE(f.campaign, '') = COALESCE(ly.campaign, '')
  AND COALESCE(f.campaign_id, '') = COALESCE(ly.campaign_id, '')
  AND COALESCE(f.source, '') = COALESCE(ly.source, '')
  AND COALESCE(f.medium, '') = COALESCE(ly.medium, '')
  AND COALESCE(f.normalized_os, '') = COALESCE(ly.normalized_os, '')
  AND COALESCE(f.partner_org, '') = COALESCE(ly.partner_org, '')
  AND COALESCE(f.distribution_model, '') = COALESCE(ly.distribution_model, '')
  AND COALESCE(f.distribution_id, '') = COALESCE(ly.distribution_id, '')
