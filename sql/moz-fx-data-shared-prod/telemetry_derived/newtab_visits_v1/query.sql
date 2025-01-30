WITH events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    category AS event_category,
    name AS event_name,
    timestamp AS event_timestamp,
    client_info,
    METADATA,
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
),
visit_metadata AS (
  SELECT
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
    submission_date,
    ANY_VALUE(client_info.client_id) AS client_id,
    ANY_VALUE(metrics.uuid.legacy_telemetry_client_id) AS legacy_telemetry_client_id,
    ANY_VALUE(metrics.uuid.legacy_telemetry_profile_group_id) AS profile_group_id,
    ANY_VALUE(normalized_os) AS normalized_os,
    ANY_VALUE(normalized_os_version) AS normalized_os_version,
    ANY_VALUE(normalized_country_code) AS country_code,
    ANY_VALUE(normalized_channel) AS channel,
    ANY_VALUE(client_info.locale) AS locale,
    ANY_VALUE(client_info.app_display_version) AS browser_version,
    "Firefox Desktop" AS browser_name,
    ANY_VALUE(metrics.string.search_engine_default_engine_id) AS default_search_engine,
    ANY_VALUE(metrics.string.search_engine_private_engine_id) AS default_private_search_engine,
    ANY_VALUE(metrics.boolean.pocket_is_signed_in) AS pocket_is_signed_in,
    ANY_VALUE(metrics.boolean.pocket_enabled) AS pocket_enabled,
    ANY_VALUE(metrics.boolean.pocket_sponsored_stories_enabled) AS pocket_sponsored_stories_enabled,
    ANY_VALUE(metrics.boolean.topsites_enabled) AS topsites_enabled,
    ANY_VALUE(metrics.boolean.topsites_sponsored_enabled) AS topsites_sponsored_enabled,
    ANY_VALUE(
      metrics.quantity.topsites_sponsored_tiles_configured
    ) AS topsites_sponsored_tiles_configured,
    ANY_VALUE(metrics.string.newtab_homepage_category) AS newtab_homepage_category,
    ANY_VALUE(metrics.string.newtab_newtab_category) AS newtab_newtab_category,
    ANY_VALUE(metrics.boolean.newtab_search_enabled) AS newtab_search_enabled,
    ANY_VALUE(metrics.boolean.newtab_weather_enabled) AS newtab_weather_widget_enabled,
    ANY_VALUE(metrics.quantity.topsites_rows) AS topsites_rows,
    ANY_VALUE(metrics.string_list.newtab_blocked_sponsors) AS newtab_blocked_sponsors,
    ANY_VALUE(ping_info.experiments) AS experiments,
    MIN(IF(event_name = "opened", event_timestamp, NULL)) AS newtab_visit_started_at,
    MIN(IF(event_name = "closed", event_timestamp, NULL)) AS newtab_visit_ended_at,
    ANY_VALUE(
      IF(event_name = "opened", mozfun.map.get_key(event_details, "source"), NULL)
    ) AS newtab_open_source,
    LOGICAL_OR(event_name IN ("click", "issued", "save")) AS had_non_impression_engagement,
    LOGICAL_OR(event_name IN ("click", "save")) AS had_non_search_engagement,
    ANY_VALUE(metrics.string_list.newtab_selected_topics) AS newtab_selected_topics,
    ANY_VALUE(
      IF(
        event_name = "opened",
        SAFE_CAST(mozfun.map.get_key(event_details, "window_inner_height") AS INT),
        NULL
      )
    ) AS newtab_window_inner_height,
    ANY_VALUE(
      IF(
        event_name = "opened",
        SAFE_CAST(mozfun.map.get_key(event_details, "window_inner_width") AS INT),
        NULL
      )
    ) AS newtab_window_inner_width,
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
    AND event_name IN ('click', 'impression', 'issued')
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
    AND event_name IN ('dismiss', 'click', 'impression')
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
    mozfun.map.get_key(event_details, "tile_id") AS pocket_tile_id,
    mozfun.map.get_key(event_details, "recommendation_id") AS pocket_recommendation_id,
    COUNTIF(
      event_name = 'save'
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS pocket_saves,
    COUNTIF(
      event_name = 'click'
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS pocket_clicks,
    COUNTIF(
      event_name = 'impression'
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS pocket_impressions,
    COUNTIF(
      event_name = 'click'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS sponsored_pocket_clicks,
    COUNTIF(
      event_name = 'click'
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS organic_pocket_clicks,
    COUNTIF(
      event_name = 'impression'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS sponsored_pocket_impressions,
    COUNTIF(
      event_name = 'impression'
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS organic_pocket_impressions,
    COUNTIF(
      event_name = 'save'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS sponsored_pocket_saves,
    COUNTIF(
      event_name = 'save'
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS organic_pocket_saves,
    COUNTIF(
      event_name = 'dismiss'
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS sponsored_pocket_dismissals,
    COUNTIF(
      event_name = 'dismiss'
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
      AND COALESCE(mozfun.map.get_key(event_details, "is_list_card"), "false") = "false"
    ) AS organic_pocket_dismissals,
    COUNTIF(
      event_name = 'thumb_voting_interaction'
      AND mozfun.map.get_key(event_details, "thumbs_up") = "true"
    ) AS pocket_thumbs_up,
    COUNTIF(
      event_name = 'thumb_voting_interaction'
      AND mozfun.map.get_key(event_details, "thumbs_down") = "true"
    ) AS pocket_thumbs_down,
    SAFE_CAST(mozfun.map.get_key(event_details, "received_rank") AS INT) AS pocket_received_rank,
    mozfun.map.get_key(
      event_details,
      "scheduled_corpus_item_id"
    ) AS pocket_scheduled_corpus_item_id,
    mozfun.map.get_key(event_details, "topic") AS pocket_topic,
    mozfun.map.get_key(event_details, "matches_selected_topic") AS pocket_matches_selected_topic,
    COUNTIF(
      event_name = "click"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
    ) AS list_card_clicks,
    COUNTIF(
      event_name = "click"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
    ) AS organic_list_card_clicks,
    COUNTIF(
      event_name = "click"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
    ) AS sponsored_list_card_clicks,
    COUNTIF(
      event_name = "impression"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
    ) AS list_card_impressions,
    COUNTIF(
      event_name = "impression"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
    ) AS organic_list_card_impressions,
    COUNTIF(
      event_name = "impression"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
    ) AS sponsored_list_card_impressions,
    COUNTIF(
      event_name = "save"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
    ) AS list_card_saves,
    COUNTIF(
      event_name = "save"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
    ) AS organic_list_card_saves,
    COUNTIF(
      event_name = "save"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
    ) AS sponsored_list_card_saves,
    COUNTIF(
      event_name = "dismiss"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
    ) AS list_card_dismissals,
    COUNTIF(
      event_name = "dismiss"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
      AND mozfun.map.get_key(event_details, "is_sponsored") != "true"
    ) AS organic_list_card_dismissals,
    COUNTIF(
      event_name = "dismiss"
      AND mozfun.map.get_key(event_details, "is_list_card") = "true"
      AND mozfun.map.get_key(event_details, "is_sponsored") = "true"
    ) AS sponsored_list_card_dismissals,
  FROM
    events_unnested
  WHERE
    event_category = 'pocket'
    AND event_name IN ('impression', 'click', 'save', 'dismiss', 'thumb_voting_interaction')
  GROUP BY
    newtab_visit_id,
    pocket_story_position,
    pocket_tile_id,
    pocket_recommendation_id,
    pocket_received_rank,
    pocket_scheduled_corpus_item_id,
    pocket_topic,
    pocket_matches_selected_topic
),
pocket_summary AS (
  SELECT
    newtab_visit_id,
    ARRAY_AGG(
      STRUCT(
        pocket_story_position,
        pocket_tile_id,
        pocket_recommendation_id,
        pocket_impressions,
        sponsored_pocket_impressions,
        organic_pocket_impressions,
        pocket_clicks,
        sponsored_pocket_clicks,
        organic_pocket_clicks,
        pocket_saves,
        sponsored_pocket_saves,
        organic_pocket_saves,
        sponsored_pocket_dismissals,
        organic_pocket_dismissals,
        pocket_thumbs_up,
        pocket_thumbs_down,
        pocket_received_rank,
        pocket_scheduled_corpus_item_id,
        pocket_topic,
        pocket_matches_selected_topic,
        list_card_clicks,
        organic_list_card_clicks,
        sponsored_list_card_clicks,
        list_card_impressions,
        organic_list_card_impressions,
        sponsored_list_card_impressions,
        list_card_saves,
        organic_list_card_saves,
        sponsored_list_card_saves,
        list_card_dismissals,
        organic_list_card_dismissals,
        sponsored_list_card_dismissals
      )
    ) AS pocket_interactions
  FROM
    pocket_events
  GROUP BY
    newtab_visit_id
),
wallpaper_events AS (
  SELECT
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
    mozfun.map.get_key(event_details, "selected_wallpaper") AS wallpaper_selected_wallpaper,
    COUNTIF(event_name = 'wallpaper_click') AS wallpaper_clicks,
    COUNTIF(
      event_name = 'wallpaper_click'
      AND mozfun.map.get_key(event_details, "had_previous_wallpaper") = "true"
    ) AS wallpaper_clicks_had_previous_wallpaper,
    COUNTIF(
      event_name = 'wallpaper_click'
      AND mozfun.map.get_key(event_details, "had_previous_wallpaper") = "false"
    ) AS wallpaper_clicks_first_selected_wallpaper,
    COUNTIF(event_name = 'wallpaper_category_click') AS wallpaper_category_clicks,
    COUNTIF(event_name = 'wallpaper_highlight_dismissed') AS wallpaper_highlight_dismissals,
    COUNTIF(event_name = 'wallpaper_highlight_cta_click') AS wallpaper_highlight_cta_clicks
  FROM
    events_unnested
  WHERE
    event_category = 'newtab'
    AND event_name IN (
      'wallpaper_click',
      'wallpaper_category_click',
      'wallpaper_highlight_cta_clicks',
      'wallpaper_highlight_dismissed'
    )
  GROUP BY
    newtab_visit_id,
    wallpaper_selected_wallpaper
),
wallpaper_summary AS (
  SELECT
    newtab_visit_id,
    ARRAY_AGG(
      STRUCT(
        wallpaper_selected_wallpaper,
        wallpaper_clicks,
        wallpaper_clicks_had_previous_wallpaper,
        wallpaper_clicks_first_selected_wallpaper,
        wallpaper_category_clicks,
        wallpaper_highlight_dismissals,
        wallpaper_highlight_cta_clicks
      )
    ) AS wallpaper_interactions
  FROM
    wallpaper_events
  GROUP BY
    newtab_visit_id
),
weather_events AS (
  SELECT
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
    COUNTIF(event_name = 'weather_impression') AS weather_widget_impressions,
    COUNTIF(event_name = 'weather_location_selected') AS weather_widget_location_selected,
    COUNTIF(event_name = 'weather_open_provider_url') AS weather_widget_clicks,
    COUNTIF(event_name = 'weather_load_error') AS weather_widget_load_errors,
    COUNTIF(
      event_name = 'weather_change_display'
      AND mozfun.map.get_key(event_details, "weather_display_mode") = "detailed"
    ) AS weather_widget_change_display_to_detailed,
    COUNTIF(
      event_name = 'weather_change_display'
      AND mozfun.map.get_key(event_details, "weather_display_mode") = "simple"
    ) AS weather_widget_change_display_to_simple
  FROM
    events_unnested
  WHERE
    event_category = 'newtab'
    AND event_name IN (
      'weather_impression',
      'weather_open_provider_url',
      'weather_load_error',
      'weather_change_display',
      'weather_location_selected'
    )
  GROUP BY
    newtab_visit_id
),
weather_summary AS (
  SELECT
    newtab_visit_id,
    ARRAY_AGG(
      STRUCT(
        weather_widget_impressions,
        weather_widget_location_selected,
        weather_widget_clicks,
        weather_widget_load_errors,
        weather_widget_change_display_to_detailed,
        weather_widget_change_display_to_simple
      )
    ) AS weather_interactions
  FROM
    weather_events
  GROUP BY
    newtab_visit_id
),
topic_selection_events AS (
  SELECT
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
    mozfun.map.get_key(event_details, "previous_topics") AS previous_topics,
    mozfun.map.get_key(event_details, "topics") AS topics,
    COUNTIF(event_name = 'topic_selection_open') AS topic_selection_open,
    COUNTIF(event_name = 'topic_selection_dismiss') AS topic_selection_dismiss,
    COUNTIF(
      event_name = 'topic_selection_topics_saved'
      AND mozfun.map.get_key(event_details, "first_save") = "true"
    ) AS topic_selection_topics_first_saved,
    COUNTIF(
      event_name = 'topic_selection_topics_saved'
      AND mozfun.map.get_key(event_details, "first_save") != "true"
    ) AS topic_selection_topics_updated,
  FROM
    events_unnested
  WHERE
    event_category = 'newtab'
    AND event_name IN (
      'topic_selection_dismiss',
      'topic_selection_open',
      'topic_selection_topics_saved'
    )
  GROUP BY
    newtab_visit_id,
    previous_topics,
    topics
),
topic_selection_summary AS (
  SELECT
    newtab_visit_id,
    ARRAY_AGG(
      STRUCT(
        previous_topics,
        topics,
        topic_selection_open,
        topic_selection_dismiss,
        topic_selection_topics_first_saved,
        topic_selection_topics_updated
      )
    ) AS topic_selection_interactions
  FROM
    topic_selection_events
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
  LEFT JOIN
    wallpaper_summary
    USING (newtab_visit_id)
  LEFT JOIN
    weather_summary
    USING (newtab_visit_id)
  LEFT JOIN
    topic_selection_summary
    USING (newtab_visit_id)
  WHERE
    -- Keep only rows with interactions, unless we receive a valid newtab.opened event.
    -- This is meant to drop only interactions that only have a newtab.closed event on the same partition
    -- (these are suspected to be from pre-loaded tabs)
    newtab_open_source IS NOT NULL
    OR search_interactions IS NOT NULL
    OR topsite_tile_interactions IS NOT NULL
    OR pocket_interactions IS NOT NULL
    OR wallpaper_interactions IS NOT NULL
    OR weather_interactions IS NOT NULL
    OR topic_selection_interactions IS NOT NULL
),
client_profile_info AS (
  SELECT
    client_id AS legacy_telemetry_client_id,
    LOGICAL_OR(first_seen_date = @submission_date) AS is_new_profile,
    ANY_VALUE(activity_segment) AS activity_segment
  FROM
    `moz-fx-data-shared-prod.telemetry.desktop_active_users`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
)
SELECT
  *,
  CASE
    WHEN (
        (newtab_open_source = "about:home" AND newtab_homepage_category = "enabled")
        OR (newtab_open_source = "about:newtab" AND newtab_newtab_category = "enabled")
      )
      THEN "default"
    ELSE "non-default"
  END AS newtab_default_ui,
FROM
  combined_newtab_activity
LEFT JOIN
  client_profile_info
  USING (legacy_telemetry_client_id)
