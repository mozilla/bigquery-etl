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
    DATE(submission_timestamp) = @submission_date
    AND category IN ('newtab', 'topsites', 'newtab.search', 'newtab.search.ad', 'pocket')
    AND name IN ('closed', 'opened', 'impression', 'issued', 'click', 'save', 'topic_click', 'dismiss')
),
visit_metadata AS (
  SELECT
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
    submission_date,
    ANY_VALUE(client_info.client_id) AS client_id,
    ANY_VALUE(metrics.uuid.legacy_telemetry_client_id) AS legacy_telemetry_client_id,
    ANY_VALUE(normalized_os) AS normalized_os,
    ANY_VALUE(normalized_os_version) AS normalized_os_version,
    ANY_VALUE(normalized_country_code) AS country_code,
    ANY_VALUE(normalized_channel) AS channel,
    ANY_VALUE(client_info.app_display_version) AS browser_version,
    "Firefox Desktop" AS browser_name,
    ANY_VALUE(metrics.string.search_engine_default_engine_id) AS default_search_engine,
    ANY_VALUE(metrics.string.search_engine_private_engine_id) AS default_private_search_engine,
    ANY_VALUE(metrics.boolean.pocket_is_signed_in) AS pocket_is_signed_in,
    ANY_VALUE(metrics.boolean.pocket_enabled) AS pocket_enabled,
    ANY_VALUE(metrics.boolean.pocket_sponsored_stories_enabled) AS pocket_sponsored_stories_enabled,
    ANY_VALUE(metrics.boolean.topsites_enabled) AS topsites_enabled,
    ANY_VALUE(metrics.string.newtab_homepage_category) AS newtab_homepage_category,
    ANY_VALUE(metrics.string.newtab_newtab_category) AS newtab_newtab_category,
    ANY_VALUE(metrics.boolean.newtab_search_enabled) AS newtab_search_enabled,
    ANY_VALUE(metrics.quantity.topsites_rows) AS topsites_rows,
    ANY_VALUE(metrics.string_list.newtab_blocked_sponsors) AS newtab_blocked_sponsors,
    ANY_VALUE(ping_info.experiments) AS experiments,
    MIN(IF(event_name = "opened", event_timestamp, NULL)) AS newtab_visit_started_at,
    MIN(IF(event_name = "closed", event_timestamp, NULL)) AS newtab_visit_ended_at,
    ANY_VALUE(
      IF(event_name = "opened", mozfun.map.get_key(event_details, "source"), NULL)
    ) AS newtab_open_source,
    LOGICAL_OR(event_name IN ("click", "issued", "save")) AS had_non_impression_engagement
  FROM
    events_unnested
  GROUP BY
    newtab_visit_id,
    submission_date
  HAVING
    newtab_visit_id IS NOT NULL
),
search_events AS (
  SELECT
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
    `moz-fx-data-shared-prod`.udf.normalize_search_engine(
      mozfun.map.get_key(event_details, "telemetry_id")
    ) AS search_engine,
    mozfun.map.get_key(event_details, "search_access_point") AS search_access_point,
    COUNTIF(event_name = "issued") AS searches,
    COUNTIF(
      event_name = "impression"
      AND mozfun.map.get_key(event_details, "is_tagged") = "true"
    ) AS tagged_search_ad_impressions,
    COUNTIF(
      event_name = "impression"
      AND mozfun.map.get_key(event_details, "is_follow_on") = "true"
    ) AS follow_on_search_ad_impressions,
    COUNTIF(
      event_name = "impression"
      AND mozfun.map.get_key(event_details, "is_follow_on") = "true"
      AND mozfun.map.get_key(event_details, "is_tagged") = "true"
    ) AS tagged_follow_on_search_ad_impressions,
    COUNTIF(
      event_name = "click"
      AND mozfun.map.get_key(event_details, "is_tagged") = "true"
    ) AS tagged_search_ad_clicks,
    COUNTIF(
      event_name = "click"
      AND mozfun.map.get_key(event_details, "is_follow_on") = "true"
    ) AS follow_on_search_ad_clicks,
    COUNTIF(
      event_name = "click"
      AND mozfun.map.get_key(event_details, "is_follow_on") = "true"
      AND mozfun.map.get_key(event_details, "is_tagged") = "true"
    ) AS tagged_follow_on_search_ad_clicks
  FROM
    events_unnested
  WHERE
    event_category IN ('newtab.search', 'newtab.search.ad')
  GROUP BY
    newtab_visit_id,
    search_engine,
    search_access_point
),
search_summary AS (
  SELECT
    newtab_visit_id,
    ARRAY_AGG(
      STRUCT(
        search_engine,
        search_access_point,
        searches,
        tagged_search_ad_clicks,
        tagged_search_ad_impressions,
        follow_on_search_ad_clicks,
        follow_on_search_ad_impressions,
        tagged_follow_on_search_ad_clicks,
        tagged_follow_on_search_ad_impressions
      )
    ) AS search_interactions
  FROM
    search_events
  GROUP BY
    newtab_visit_id
),
topsites_events AS (
  SELECT
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
    SAFE_CAST(mozfun.map.get_key(event_details, "position") AS INT64) AS topsite_tile_position,
    mozfun.map.get_key(event_details, "advertiser_name") AS topsite_tile_advertiser_name,
    mozfun.map.get_key(event_details, "tile_id") AS topsite_tile_id,
    JSON_EXTRACT(sov, "$.assigned") AS topsite_tile_assigned_sov_branch,
    JSON_EXTRACT(sov, "$.chosen") AS topsite_tile_displayed_sov_branch,
    COUNTIF(event_name = 'impression') AS topsite_tile_impressions,
    COUNTIF(event_name = 'click') AS topsite_tile_clicks,
    COUNTIF(
      event_name = 'impression'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
    ) AS sponsored_topsite_tile_impressions,
    COUNTIF(
      event_name = 'impression'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "false"
    ) AS organic_topsite_tile_impressions,
    COUNTIF(
      event_name = 'click'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
    ) AS sponsored_topsite_tile_clicks,
    COUNTIF(
      event_name = 'click'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "false"
    ) AS organic_topsite_tile_clicks,
    COUNTIF(event_name = 'dismiss') AS topsite_tile_dismissals,
    COUNTIF(
      event_name = 'dismiss'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
    ) AS sponsored_topsite_tile_dismissals,
    COUNTIF(
      event_name = 'dismiss'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "false"
    ) AS organic_topsite_tile_dismissals,
  FROM
    events_unnested
  LEFT JOIN
    UNNEST(metrics.string_list.newtab_sov_allocation) sov
    ON SAFE_CAST(mozfun.map.get_key(event_details, "position") AS INT64) = SAFE_CAST(
      JSON_EXTRACT(sov, "$.pos") AS INT64
    )
  WHERE
    event_category = 'topsites'
  GROUP BY
    newtab_visit_id,
    topsite_tile_position,
    topsite_tile_advertiser_name,
    topsite_tile_id,
    topsite_tile_assigned_sov_branch,
    topsite_tile_displayed_sov_branch
),
topsites_summary AS (
  SELECT
    newtab_visit_id,
    ARRAY_AGG(
      STRUCT(
        topsite_tile_advertiser_name,
        topsite_tile_position,
        topsite_tile_id,
        topsite_tile_assigned_sov_branch,
        topsite_tile_displayed_sov_branch,
        topsite_tile_clicks,
        sponsored_topsite_tile_clicks,
        organic_topsite_tile_clicks,
        topsite_tile_impressions,
        sponsored_topsite_tile_impressions,
        organic_topsite_tile_impressions,
        topsite_tile_dismissals,
        sponsored_topsite_tile_dismissals,
        organic_topsite_tile_dismissals
      )
    ) AS topsite_tile_interactions
  FROM
    topsites_events
  GROUP BY
    newtab_visit_id
),
pocket_events AS (
  SELECT
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
    SAFE_CAST(mozfun.map.get_key(event_details, "position") AS INT64) AS pocket_story_position,
    COUNTIF(event_name = 'save') AS pocket_saves,
    COUNTIF(event_name = 'click') AS pocket_clicks,
    COUNTIF(event_name = 'impression') AS pocket_impressions,
    COUNTIF(
      event_name = 'click'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
    ) AS sponsored_pocket_clicks,
    COUNTIF(
      event_name = 'click'
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
    ) AS organic_pocket_clicks,
    COUNTIF(
      event_name = 'impression'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
    ) AS sponsored_pocket_impressions,
    COUNTIF(
      event_name = 'impression'
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
    ) AS organic_pocket_impressions,
    COUNTIF(
      event_name = 'save'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
    ) AS sponsored_pocket_saves,
    COUNTIF(
      event_name = 'save'
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
    ) AS organic_pocket_saves,
  FROM
    events_unnested
  WHERE
    event_category = 'pocket'
  GROUP BY
    newtab_visit_id,
    pocket_story_position
),
pocket_summary AS (
  SELECT
    newtab_visit_id,
    ARRAY_AGG(
      STRUCT(
        pocket_story_position,
        pocket_impressions,
        sponsored_pocket_impressions,
        organic_pocket_impressions,
        pocket_clicks,
        sponsored_pocket_clicks,
        organic_pocket_clicks,
        pocket_saves,
        sponsored_pocket_saves,
        organic_pocket_saves
      )
    ) AS pocket_interactions
  FROM
    pocket_events
  GROUP BY
    newtab_visit_id
),
combined_newtab_activity AS (
  SELECT
    *
  FROM
    visit_metadata
  LEFT JOIN
    search_summary
    USING (newtab_visit_id)
  LEFT JOIN
    topsites_summary
    USING (newtab_visit_id)
  LEFT JOIN
    pocket_summary
    USING (newtab_visit_id)
  WHERE
   -- Keep only rows with interactions, unless we receive a valid newtab.opened event.
   -- This is meant to drop only interactions that only have a newtab.closed event on the same partition
   -- (these are suspected to be from pre-loaded tabs)
    newtab_open_source IS NOT NULL
    OR search_interactions IS NOT NULL
    OR topsite_tile_interactions IS NOT NULL
    OR pocket_interactions IS NOT NULL
),
client_profile_info AS (
  SELECT
    client_id AS legacy_telemetry_client_id,
    ANY_VALUE(is_new_profile) AS is_new_profile,
    ANY_VALUE(activity_segment) AS activity_segment
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
)
SELECT
  *
FROM
  combined_newtab_activity
LEFT JOIN
  client_profile_info
  USING (legacy_telemetry_client_id)
