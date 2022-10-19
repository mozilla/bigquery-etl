WITH events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    category AS event_category,
    name AS event_name,
    timestamp AS event_timestamp,
    client_info,
    metadata,
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    normalized_channel,
    ping_info,
    extra AS event_details,
    metrics
  FROM
    -- https://dictionary.telemetry.mozilla.org/apps/firefox_desktop/pings/newtab
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = DATE('2022-09-12')
    AND category IN ('newtab', 'topsites', 'newtab.search', 'newtab.search.ad', 'pocket')
    AND name IN ('closed', 'opened', 'impression', 'issued', 'click', 'save', 'topic_click')
),
categorized_events AS (
  SELECT
        -- Unique Identifiers
    client_info.client_id,
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
        -- Metrics
        -- Search
    event_name = "issued"
    AND event_category = "newtab.search" AS is_search_issued,
        -- ??? is_tagged_search
        -- ??? is_follow_on_search
    event_name = "impression"
    AND event_category = 'newtab.search.ad'
    AND mozfun.map.get_key(event_details, "is_tagged") = "true" AS is_tagged_search_ad_impression,
    event_name = "impression"
    AND event_category = 'newtab.search.ad'
    AND mozfun.map.get_key(
      event_details,
      "is_follow_on"
    ) = "true" AS is_follow_on_search_ad_impression,
    event_name = "click"
    AND event_category = 'newtab.search.ad'
    AND mozfun.map.get_key(event_details, "is_tagged") = "true" AS is_tagged_search_ad_click,
    event_name = "click"
    AND event_category = 'newtab.search.ad'
    AND mozfun.map.get_key(event_details, "is_follow_on") = "true" AS is_follow_on_search_ad_click,
        -- Topsites
    event_category = 'topsites'
    AND event_name = 'impression' AS is_topsite_impression,
    event_category = 'topsites'
    AND event_name = 'impression'
    AND mozfun.map.get_key(
      event_details,
      "is_sponsored"
    ) = "true" AS is_sponsored_topsite_impression,
    event_category = 'topsites'
    AND event_name = 'click' AS is_topsite_click,
    event_category = 'topsites'
    AND event_name = 'click'
    AND mozfun.map.get_key(event_details, "is_sponsored") = "true" AS is_sponsored_topsite_click,
        -- Pocket
    event_category = 'pocket'
    AND event_name = 'click' AS is_pocket_click,
    event_category = 'pocket'
    AND event_name = 'click'
    AND mozfun.map.get_key(event_details, "is_sponsored") = "true" AS is_sponsored_pocket_click,
    event_category = 'pocket'
    AND event_name = 'click'
    AND mozfun.map.get_key(event_details, "is_sponsored") != "true" AS is_organic_pocket_click,
    event_category = 'pocket'
    AND event_name = 'impression' AS is_pocket_impression,
    event_category = 'pocket'
    AND event_name = 'impression'
    AND mozfun.map.get_key(
      event_details,
      "is_sponsored"
    ) = "true" AS is_sponsored_pocket_impression,
    event_category = 'pocket'
    AND event_name = 'impression'
    AND mozfun.map.get_key(event_details, "is_sponsored") != "true" AS is_organic_pocket_impression,
    event_category = 'pocket'
    AND event_name = 'save' AS is_pocket_save,
    event_category = 'pocket'
    AND event_name = 'save'
    AND mozfun.map.get_key(event_details, "is_sponsored") = "true" AS is_sponsored_pocket_save,
    event_category = 'pocket'
    AND event_name = 'save'
    AND mozfun.map.get_key(event_details, "is_sponsored") != "true" AS is_organic_pocket_save,
    IF(event_name = "opened", event_timestamp, NULL) AS newtab_visit_started_at,
    IF(event_name = "closed", event_timestamp, NULL) AS newtab_visit_ended_at,
        -- Client/Visit-unique attributes
    normalized_os,
    normalized_os_version,
    normalized_country_code,
    normalized_channel,
    client_info.app_display_version,
    mozfun.map.get_key(event_details, "source") AS newtab_open_source,
    metrics.string.search_engine_private_engine_id AS default_search_engine,
    metrics.string.search_engine_default_engine_id AS default_private_search_engine,
    metrics.boolean.pocket_is_signed_in,
    metrics.boolean.pocket_enabled,
    metrics.boolean.pocket_sponsored_stories_enabled,
    metrics.boolean.topsites_enabled,
    metrics.string.newtab_homepage_category,
    metrics.string.newtab_newtab_category,
    metrics.boolean.newtab_search_enabled,
    ping_info.experiments,
        -- ??? private_browsing_mode
        -- Partially unique visit attributes
    mozfun.map.get_key(event_details, "telemetry_id") AS search_engine,
    mozfun.map.get_key(event_details, "search_access_point") AS search_access_point,
    SAFE_CAST(
      mozfun.map.get_key(event_details, "position") AS INT64
    ) AS pocket_story_position, -- Note potential name-collision here with topsite position
        -- ??? topsite_advertiser_id
        -- ??? topsite_position
    submission_date
  FROM
    events_unnested
)
SELECT
  newtab_visit_id,
  client_id,
  submission_date,
  search_engine,
  search_access_point,
  pocket_story_position,
    -- topsite_advertiser_id,
    -- topsite_position,
  ANY_VALUE(experiments) AS experiments,
  ANY_VALUE(default_private_search_engine) AS default_private_search_engine,
  ANY_VALUE(default_search_engine) AS default_search_engine,
  ANY_VALUE(normalized_os) AS os,
  ANY_VALUE(normalized_os_version) AS os_version,
  ANY_VALUE(normalized_country_code) AS country_code,
  ANY_VALUE(normalized_channel) AS channel,
  ANY_VALUE(app_display_version) AS browser_version,
  "Firefox Desktop" AS browser_name,
  ANY_VALUE(newtab_open_source) AS newtab_open_source,
  ANY_VALUE(pocket_is_signed_in) AS pocket_is_signed_in,
  ANY_VALUE(pocket_enabled) AS pocket_enabled,
  ANY_VALUE(pocket_sponsored_stories_enabled) AS pocket_sponsored_stories_enabled,
  ANY_VALUE(topsites_enabled) AS topsites_enabled,
  ANY_VALUE(newtab_homepage_category) AS newtab_homepage_category,
  ANY_VALUE(newtab_newtab_category) AS newtab_newtab_category,
  ANY_VALUE(newtab_search_enabled) AS newtab_search_enabled,
  MIN(newtab_visit_started_at) AS newtab_visit_started_at,
  MIN(newtab_visit_ended_at) AS newtab_visit_ended_at,
      -- Topsites
  COUNTIF(is_topsite_click) AS topsite_clicks,
  COUNTIF(is_sponsored_topsite_click) AS sponsored_topsite_clicks,
  COUNTIF(is_topsite_impression) AS topsite_impressions,
  COUNTIF(is_sponsored_topsite_impression) AS sponsored_topsite_impressions,
      -- Search
  COUNTIF(is_search_issued) AS searches,
  COUNTIF(is_tagged_search_ad_click) AS tagged_search_ad_clicks,
  COUNTIF(is_tagged_search_ad_impression) AS tagged_search_ad_impressions,
  COUNTIF(is_follow_on_search_ad_click) AS follow_on_search_ad_clicks,
  COUNTIF(is_follow_on_search_ad_impression) AS follow_on_search_ad_impressions,
  COUNTIF(
    is_tagged_search_ad_click
    AND is_follow_on_search_ad_click
  ) AS tagged_follow_on_search_ad_clicks,
  COUNTIF(
    is_tagged_search_ad_impression
    AND is_follow_on_search_ad_impression
  ) AS tagged_follow_on_search_ad_impressions,
      -- Pocket
  COUNTIF(is_pocket_impression) AS pocket_impressions,
  COUNTIF(is_sponsored_pocket_impression) AS sponsored_pocket_impressions,
  COUNTIF(is_organic_pocket_impression) AS organic_pocket_impressions,
  COUNTIF(is_pocket_click) AS pocket_clicks,
  COUNTIF(is_sponsored_pocket_click) AS sponsored_pocket_clicks,
  COUNTIF(is_organic_pocket_click) AS organic_pocket_clicks,
  COUNTIF(is_pocket_save) AS pocket_saves,
  COUNTIF(is_sponsored_pocket_save) AS sponsored_pocket_saves,
  COUNTIF(is_organic_pocket_save) AS organic_pocket_saves,
FROM
  categorized_events
GROUP BY
  newtab_visit_id,
  client_id,
  submission_date,
  search_engine,
  search_access_point,
  pocket_story_position
    -- topsite_advertiser_id,
    -- topsite_position
