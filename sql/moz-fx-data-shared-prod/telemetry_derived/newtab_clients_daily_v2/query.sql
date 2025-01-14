WITH visits_data_base AS (
  SELECT
    client_id,
    submission_date,
    legacy_telemetry_client_id,
    newtab_visit_id,
    normalized_os,
    normalized_os_version,
    country_code,
    locale,
    channel,
    browser_version,
    browser_name,
    default_search_engine,
    default_private_search_engine,
    pocket_is_signed_in,
    pocket_enabled,
    pocket_sponsored_stories_enabled,
    topsites_enabled,
    topsites_sponsored_enabled,
    newtab_weather_widget_enabled,
    newtab_homepage_category,
    newtab_newtab_category,
    topsites_rows,
    experiments,
    had_non_impression_engagement,
    had_non_search_engagement,
    is_new_profile,
    activity_segment,
    search_interactions,
    topsite_tile_interactions,
    pocket_interactions,
    wallpaper_interactions,
    weather_interactions,
    topic_selection_interactions,
    default_ui,
    newtab_selected_topics,
    profile_group_id,
    topsites_sponsored_tiles_configured,
    newtab_open_source
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.newtab_visits_v1`
  WHERE
    submission_date = @submission_date
),
visits_data AS (
  SELECT
    client_id,
    submission_date,
    ANY_VALUE(profile_group_id) AS profile_group_id,
    ANY_VALUE(legacy_telemetry_client_id) AS legacy_telemetry_client_id,
    COUNT(newtab_visit_id) AS newtab_visit_count,
    ANY_VALUE(normalized_os) AS normalized_os,
    ANY_VALUE(normalized_os_version) AS normalized_os_version,
    ANY_VALUE(country_code) AS country_code,
    ANY_VALUE(locale) AS locale,
    ANY_VALUE(channel) AS channel,
    ANY_VALUE(browser_version) AS browser_version,
    ANY_VALUE(browser_name) AS browser_name,
    ANY_VALUE(default_search_engine) AS default_search_engine,
    ANY_VALUE(default_private_search_engine) AS default_private_search_engine,
    LOGICAL_OR(pocket_is_signed_in) AS pocket_is_signed_in,
    LOGICAL_AND(pocket_enabled) AS pocket_enabled,
    LOGICAL_AND(pocket_sponsored_stories_enabled) AS pocket_sponsored_stories_enabled,
    LOGICAL_AND(topsites_enabled) AS topsites_enabled,
    LOGICAL_AND(topsites_sponsored_enabled) AS topsites_sponsored_enabled,
    LOGICAL_AND(newtab_weather_widget_enabled) AS newtab_weather_widget_enabled,
    ANY_VALUE(newtab_homepage_category) AS newtab_homepage_category,
    ANY_VALUE(newtab_newtab_category) AS newtab_newtab_category,
    ANY_VALUE(topsites_rows) AS topsites_rows,
    ANY_VALUE(experiments) AS experiments,
    COUNTIF(had_non_impression_engagement) AS visits_with_non_impression_engagement,
    COUNTIF(had_non_search_engagement) AS visits_with_non_search_engagement,
    COUNTIF(default_ui = "default") AS visits_with_default_ui,
    COUNTIF(default_ui = "default" AND had_non_impression_engagement) AS visits_with_default_ui_with_non_impression_engagement,
    COUNTIF(default_ui = "default" AND had_non_search_engagement) AS visits_with_default_ui_with_non_search_engagement,
    COUNTIF(default_ui = "home - non-default" OR default_ui = "newtab - non-default") AS visits_with_non_default_ui,
    LOGICAL_OR(is_new_profile) AS is_new_profile,
    ANY_VALUE(activity_segment) AS activity_segment,
    LOGICAL_OR(ARRAY_LENGTH(newtab_selected_topics) > 0) AS topic_preferences_set,
    ANY_VALUE(topsites_sponsored_tiles_configured) AS topsites_sponsored_tiles_configured
  FROM
    visits_data_base
  GROUP BY
    client_id,
    submission_date
),
search_data AS (
  SELECT
    client_id,
    SUM(searches) AS searches,
    SUM(tagged_search_ad_clicks) AS tagged_search_ad_clicks,
    SUM(tagged_search_ad_impressions) AS tagged_search_ad_impressions,
    SUM(follow_on_search_ad_clicks) AS follow_on_search_ad_clicks,
    SUM(follow_on_search_ad_impressions) AS follow_on_search_ad_impressions,
    SUM(tagged_follow_on_search_ad_clicks) AS tagged_follow_on_search_ad_clicks,
    SUM(tagged_follow_on_search_ad_impressions) AS tagged_follow_on_search_ad_impressions,
  FROM
    visits_data_base
  CROSS JOIN
    UNNEST(search_interactions)
  GROUP BY
    client_id
),
tiles_data AS (
  SELECT
    client_id,
    SUM(topsite_tile_clicks) AS topsite_tile_clicks,
    SUM(sponsored_topsite_tile_clicks) AS sponsored_topsite_tile_clicks,
    SUM(organic_topsite_tile_clicks) AS organic_topsite_tile_clicks,
    SUM(topsite_tile_impressions) AS topsite_tile_impressions,
    SUM(sponsored_topsite_tile_impressions) AS sponsored_topsite_tile_impressions,
    SUM(organic_topsite_tile_impressions) AS organic_topsite_tile_impressions,
    SUM(topsite_tile_dismissals) AS topsite_tile_dismissals,
    SUM(sponsored_topsite_tile_dismissals) AS sponsored_topsite_tile_dismissals,
    SUM(organic_topsite_tile_dismissals) AS organic_topsite_tile_dismissals,
  FROM
    visits_data_base
  CROSS JOIN
    UNNEST(topsite_tile_interactions)
  GROUP BY
    client_id
),
pocket_data AS (
  SELECT
    client_id,
    SUM(pocket_impressions) AS pocket_impressions,
    SUM(sponsored_pocket_impressions) AS sponsored_pocket_impressions,
    SUM(organic_pocket_impressions) AS organic_pocket_impressions,
    SUM(pocket_clicks) AS pocket_clicks,
    SUM(sponsored_pocket_clicks) AS sponsored_pocket_clicks,
    SUM(organic_pocket_clicks) AS organic_pocket_clicks,
    SUM(pocket_saves) AS pocket_saves,
    SUM(sponsored_pocket_saves) AS sponsored_pocket_saves,
    SUM(organic_pocket_saves) AS organic_pocket_saves,
    SUM(sponsored_pocket_dismissals) AS sponsored_pocket_dismissals,
    SUM(organic_pocket_dismissals) AS organic_pocket_dismissals,
    SUM(pocket_thumbs_up) AS pocket_thumbs_up,
    SUM(pocket_thumbs_down) AS pocket_thumbs_down,
    SUM(pocket_thumbs_down) + SUM(pocket_thumbs_up) AS pocket_thumb_voting_events,
    SUM(list_card_clicks) AS list_card_clicks,
    SUM(organic_list_card_clicks) AS organic_list_card_clicks,
    SUM(sponsored_list_card_clicks) AS sponsored_list_card_clicks,
    SUM(list_card_impressions) AS list_card_impressions,
    SUM(organic_list_card_impressions) AS organic_list_card_impressions,
    SUM(sponsored_list_card_impressions) AS sponsored_list_card_impressions,
    SUM(list_card_saves) AS list_card_saves,
    SUM(organic_list_card_saves) AS organic_list_card_saves,
    SUM(sponsored_list_card_saves) AS sponsored_list_card_saves,
    SUM(list_card_dismissals) AS list_card_dismissals,
    SUM(organic_list_card_dismissals) AS organic_list_card_dismissals,
    SUM(sponsored_list_card_dismissals) AS sponsored_list_card_dismissals,
  FROM
    visits_data_base
  CROSS JOIN
    UNNEST(pocket_interactions)
  GROUP BY
    client_id
),
wallpaper_data AS (
  SELECT
    client_id,
    SUM(wallpaper_clicks) AS wallpaper_clicks,
    SUM(wallpaper_clicks_had_previous_wallpaper) AS wallpaper_clicks_had_previous_wallpaper,
    SUM(wallpaper_clicks_first_selected_wallpaper) AS wallpaper_clicks_first_selected_wallpaper,
    SUM(wallpaper_category_clicks) AS wallpaper_category_clicks,
    SUM(wallpaper_highlight_dismissals) AS wallpaper_highlight_dismissals,
    SUM(wallpaper_highlight_cta_clicks) AS wallpaper_highlight_cta_clicks
  FROM
    visits_data_base
  CROSS JOIN
    UNNEST(wallpaper_interactions)
  GROUP BY
    client_id
),
weather_data AS (
  SELECT
    client_id,
    SUM(weather_widget_impressions) AS weather_widget_impressions,
    SUM(weather_widget_clicks) AS weather_widget_clicks,
    SUM(weather_widget_load_errors) AS weather_widget_load_errors,
    SUM(weather_widget_change_display_to_detailed) AS weather_widget_change_display_to_detailed,
    SUM(weather_widget_change_display_to_simple) AS weather_widget_change_display_to_simple,
    SUM(weather_widget_location_selected) AS weather_widget_location_selected
  FROM
    visits_data_base
  CROSS JOIN
    UNNEST(weather_interactions)
  GROUP BY
    client_id
),
topic_selection_data AS (
  SELECT
    client_id,
    LOGICAL_OR(
      topic_selection_topics_first_saved > 0
    ) AS topic_selection_selected_topics_first_time,
    SUM(topic_selection_topics_updated) AS topic_selection_updates,
    SUM(topic_selection_open) AS topic_selection_opened,
    SUM(topic_selection_dismiss) AS topic_selection_dismissals
  FROM
    visits_data_base
  CROSS JOIN
    UNNEST(topic_selection_interactions)
  GROUP BY
    client_id
),
joined AS (
  SELECT
    visits_data.client_id,
    visits_data.submission_date,
    visits_data.legacy_telemetry_client_id,
    visits_data.newtab_visit_count,
    visits_data.normalized_os,
    visits_data.normalized_os_version,
    visits_data.country_code,
    visits_data.locale,
    visits_data.channel,
    visits_data.browser_version,
    visits_data.browser_name,
    visits_data.default_search_engine,
    visits_data.default_private_search_engine,
    visits_data.pocket_is_signed_in,
    visits_data.pocket_enabled,
    visits_data.pocket_sponsored_stories_enabled,
    visits_data.topsites_enabled,
    visits_data.topsites_sponsored_enabled,
    visits_data.newtab_weather_widget_enabled,
    visits_data.newtab_homepage_category,
    visits_data.newtab_newtab_category,
    visits_data.topsites_rows,
    visits_data.experiments,
    visits_data.visits_with_non_impression_engagement,
    visits_data.visits_with_non_search_engagement,
    visits_data.visits_with_non_default_ui,
    visits_data.is_new_profile,
    visits_data.activity_segment,
    -- COALESCE calls for visits where no interactions with a surface were performed and are all Null
    COALESCE(search_data.searches, 0) AS searches,
    COALESCE(search_data.tagged_search_ad_clicks, 0) AS tagged_search_ad_clicks,
    COALESCE(search_data.tagged_search_ad_impressions, 0) AS tagged_search_ad_impressions,
    COALESCE(search_data.follow_on_search_ad_clicks, 0) AS follow_on_search_ad_clicks,
    COALESCE(search_data.follow_on_search_ad_impressions, 0) AS follow_on_search_ad_impressions,
    COALESCE(search_data.tagged_follow_on_search_ad_clicks, 0) AS tagged_follow_on_search_ad_clicks,
    COALESCE(
      search_data.tagged_follow_on_search_ad_impressions,
      0
    ) AS tagged_follow_on_search_ad_impressions,
    COALESCE(tiles_data.topsite_tile_clicks, 0) AS topsite_tile_clicks,
    COALESCE(tiles_data.sponsored_topsite_tile_clicks, 0) AS sponsored_topsite_tile_clicks,
    COALESCE(tiles_data.organic_topsite_tile_clicks, 0) AS organic_topsite_tile_clicks,
    COALESCE(tiles_data.topsite_tile_impressions, 0) AS topsite_tile_impressions,
    COALESCE(
      tiles_data.sponsored_topsite_tile_impressions,
      0
    ) AS sponsored_topsite_tile_impressions,
    COALESCE(tiles_data.organic_topsite_tile_impressions, 0) AS organic_topsite_tile_impressions,
    COALESCE(tiles_data.topsite_tile_dismissals, 0) AS topsite_tile_dismissals,
    COALESCE(tiles_data.sponsored_topsite_tile_dismissals, 0) AS sponsored_topsite_tile_dismissals,
    COALESCE(tiles_data.organic_topsite_tile_dismissals, 0) AS organic_topsite_tile_dismissals,
    COALESCE(pocket_data.pocket_impressions, 0) AS pocket_impressions,
    COALESCE(pocket_data.sponsored_pocket_impressions, 0) AS sponsored_pocket_impressions,
    COALESCE(pocket_data.organic_pocket_impressions, 0) AS organic_pocket_impressions,
    COALESCE(pocket_data.pocket_clicks, 0) AS pocket_clicks,
    COALESCE(pocket_data.sponsored_pocket_clicks, 0) AS sponsored_pocket_clicks,
    COALESCE(pocket_data.organic_pocket_clicks, 0) AS organic_pocket_clicks,
    COALESCE(pocket_data.pocket_saves, 0) AS pocket_saves,
    COALESCE(pocket_data.sponsored_pocket_saves, 0) AS sponsored_pocket_saves,
    COALESCE(pocket_data.organic_pocket_saves, 0) AS organic_pocket_saves,
    COALESCE(wallpaper_clicks, 0) AS wallpaper_clicks,
    COALESCE(wallpaper_clicks_had_previous_wallpaper, 0) AS wallpaper_clicks_had_previous_wallpaper,
    COALESCE(
      wallpaper_clicks_first_selected_wallpaper,
      0
    ) AS wallpaper_clicks_first_selected_wallpaper,
    COALESCE(wallpaper_data.wallpaper_category_clicks, 0) AS wallpaper_category_clicks,
    COALESCE(wallpaper_data.wallpaper_highlight_dismissals, 0) AS wallpaper_highlight_dismissals,
    COALESCE(wallpaper_data.wallpaper_highlight_cta_clicks, 0) AS wallpaper_highlight_cta_clicks,
    COALESCE(weather_data.weather_widget_impressions, 0) AS weather_widget_impressions,
    COALESCE(weather_data.weather_widget_clicks, 0) AS weather_widget_clicks,
    COALESCE(weather_data.weather_widget_load_errors, 0) AS weather_widget_load_errors,
    COALESCE(
      weather_data.weather_widget_change_display_to_detailed,
      0
    ) AS weather_widget_change_display_to_detailed,
    COALESCE(
      weather_data.weather_widget_change_display_to_simple,
      0
    ) AS weather_widget_change_display_to_simple,
    COALESCE(weather_data.weather_widget_location_selected, 0) AS weather_widget_location_selected,
    COALESCE(visits_data.visits_with_default_ui, 0) AS visits_with_default_ui,
    COALESCE(
      visits_data.visits_with_default_ui_with_non_impression_engagement,
      0
    ) AS visits_with_default_ui_with_non_impression_engagement,
    COALESCE(
      visits_data.visits_with_default_ui_with_non_search_engagement,
      0
    ) AS visits_with_default_ui_with_non_search_engagement,
    COALESCE(visits_data.topic_preferences_set, FALSE) AS topic_preferences_set,
    COALESCE(pocket_data.sponsored_pocket_dismissals, 0) AS sponsored_pocket_dismissals,
    COALESCE(pocket_data.organic_pocket_dismissals, 0) AS organic_pocket_dismissals,
    COALESCE(pocket_data.pocket_thumbs_up, 0) AS pocket_thumbs_up,
    COALESCE(pocket_data.pocket_thumbs_down, 0) AS pocket_thumbs_down,
    COALESCE(pocket_data.pocket_thumb_voting_events, 0) AS pocket_thumb_voting_events,
    COALESCE(
      topic_selection_data.topic_selection_selected_topics_first_time,
      FALSE
    ) AS topic_selection_selected_topics_first_time,
    COALESCE(topic_selection_data.topic_selection_updates, 0) AS topic_selection_updates,
    COALESCE(topic_selection_data.topic_selection_opened, 0) AS topic_selection_opened,
    COALESCE(topic_selection_data.topic_selection_dismissals, 0) AS topic_selection_dismissals,
    visits_data.profile_group_id AS profile_group_id,
    visits_data.topsites_sponsored_tiles_configured AS topsites_sponsored_tiles_configured,
    COALESCE(pocket_data.list_card_clicks, 0) AS list_card_clicks,
    COALESCE(pocket_data.organic_list_card_clicks, 0) AS organic_list_card_clicks,
    COALESCE(pocket_data.sponsored_list_card_clicks, 0) AS sponsored_list_card_clicks,
    COALESCE(pocket_data.list_card_impressions, 0) AS list_card_impressions,
    COALESCE(pocket_data.organic_list_card_impressions, 0) AS organic_list_card_impressions,
    COALESCE(pocket_data.sponsored_list_card_impressions, 0) AS sponsored_list_card_impressions,
    COALESCE(pocket_data.list_card_saves, 0) AS list_card_saves,
    COALESCE(pocket_data.organic_list_card_saves, 0) AS organic_list_card_saves,
    COALESCE(pocket_data.sponsored_list_card_saves, 0) AS sponsored_list_card_saves,
    COALESCE(pocket_data.list_card_dismissals, 0) AS list_card_dismissals,
    COALESCE(pocket_data.organic_list_card_dismissals, 0) AS organic_list_card_dismissals,
    COALESCE(pocket_data.sponsored_list_card_dismissals, 0) AS sponsored_list_card_dismissals,
  FROM
    visits_data
  LEFT JOIN
    search_data
    USING (client_id)
  LEFT JOIN
    tiles_data
    USING (client_id)
  LEFT JOIN
    pocket_data
    USING (client_id)
  LEFT JOIN
    weather_data
    USING (client_id)
  LEFT JOIN
    wallpaper_data
    USING (client_id)
  LEFT JOIN
    topic_selection_data
    USING (client_id)
)
SELECT
  *,
  topsite_tile_clicks + pocket_clicks + pocket_saves + wallpaper_clicks + pocket_thumb_voting_events + topic_selection_updates + topic_selection_opened + CAST(
    topic_selection_selected_topics_first_time AS INT
  ) + weather_widget_clicks + weather_widget_change_display_to_detailed + weather_widget_change_display_to_simple + topsite_tile_dismissals + organic_pocket_dismissals + sponsored_pocket_dismissals + topic_selection_dismissals AS non_search_engagement_count,
  topsite_tile_dismissals + organic_pocket_dismissals + sponsored_pocket_dismissals + topic_selection_dismissals AS newtab_dismissal_count
FROM
  joined
