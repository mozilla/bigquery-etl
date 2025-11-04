WITH events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    SAFE_CAST(
      mozfun.norm.browser_version_info(client_info.app_display_version).major_version AS INT64
    ) AS app_version,
    normalized_os AS os,
    normalized_channel AS channel,
    client_info.locale AS locale,
    normalized_country_code AS country,
    sample_id,
    metrics.string.newtab_homepage_category AS homepage_category,
    metrics.string.newtab_newtab_category AS newtab_category,
    metrics.boolean.pocket_enabled AS organic_content_enabled,
    metrics.boolean.pocket_sponsored_stories_enabled AS sponsored_content_enabled,
    metrics.boolean.topsites_sponsored_enabled AS sponsored_topsites_enabled,
    metrics.boolean.topsites_enabled AS organic_topsites_enabled,
    metrics.boolean.newtab_search_enabled AS newtab_search_enabled,
    metrics.boolean.newtab_weather_enabled AS newtab_weather_enabled,
    metrics.uuid.legacy_telemetry_profile_group_id AS profile_group_id,
    metadata.geo.subdivision1 AS geo_subdivision,
    metrics.string.search_engine_default_engine_id AS default_search_engine,
    metrics.string.search_engine_private_engine_id AS default_private_search_engine,
    metrics.quantity.topsites_rows AS topsite_rows,
    metrics.quantity.topsites_sponsored_tiles_configured AS topsite_sponsored_tiles_configured,
    metrics.string_list.newtab_blocked_sponsors AS newtab_blocked_sponsors,
    ping_info AS ping_info,
    mozfun.newtab.is_default_ui_v1(
      category,
      name,
      extra,
      metrics.string.newtab_homepage_category,
      metrics.string.newtab_newtab_category
    ) AS is_default_ui,
    category AS event_category,
    name AS event_name,
    timestamp AS event_timestamp,
    extra AS event_details,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
    -- selecting the events we need to compute on
    AND (
      (category = 'newtab.search' AND name IN ('issued'))
      OR (category = 'newtab.search.ad' AND name IN ('click', 'impression'))
      OR (
        category = 'pocket'
        AND name IN ('click', 'impression', 'dismiss', 'thumb_voting_interaction')
      )
      OR (category = 'topsites' AND name IN ('click', 'impression', 'dismiss', 'pin', 'unpin'))
      OR (
        category = 'newtab'
        AND name IN (
          'opened',
          'closed',
          -- widgets
          'weather_change_display',
          'weather_open_provider_url',
          'weather_location_selected',
          'weather_impression',
          'weather_load_error',
          'widgets_lists_user_event',
          'widgets_lists_change_display',
          'widgets_timer_change_display',
          'widgets_timer_toggle_notification',
          'widgets_timer_user_event',
          'widgets_lists_impression',
          'widgets_timer_impression',
          -- wallpaper
          'wallpaper_click',
          'wallpaper_category_click',
          'wallpaper_highlight_cta_click',
          'wallpaper_highlight_dismissed',
          -- topic_selection
          'topic_selection_open',
          'topic_selection_dismiss',
          'topic_selection_topics_saved',
          -- section
          'sections_block_section',
          'sections_follow_section',
          'sections_unblock_section',
          'sections_unfollow_section',
          'sections_impression',
          'inline_selection_click',
          'inline_selection_impression'
        )
      )
    )
),
core_visit_metrics AS (
  SELECT
    submission_date,
    client_id,
    mozfun.map.get_key(event_details, 'newtab_visit_id') AS newtab_visit_id,
      -- ANY_VALUE: because we do not expect those fields to differ within a visit
    ANY_VALUE(app_version) AS app_version,
    ANY_VALUE(os) AS os,
    ANY_VALUE(channel) AS channel,
    ANY_VALUE(locale) AS locale,
    ANY_VALUE(country) AS country,
    ANY_VALUE(homepage_category) AS homepage_category,
    ANY_VALUE(newtab_category) AS newtab_category,
    ANY_VALUE(organic_content_enabled) AS organic_content_enabled,
    ANY_VALUE(sponsored_content_enabled) AS sponsored_content_enabled,
    ANY_VALUE(sponsored_topsites_enabled) AS sponsored_topsites_enabled,
    ANY_VALUE(organic_topsites_enabled) AS organic_topsites_enabled,
    ANY_VALUE(newtab_search_enabled) AS newtab_search_enabled,
    LOGICAL_OR(is_default_ui) AS is_default_ui,
    LOGICAL_OR(event_category = 'newtab' AND event_name = 'opened') AS is_newtab_opened,
      -- The computations below that use "LOGICAL_OR(is_default_ui)", only includes visits opened in default ui
      -- Newtab open is an independent event that needs to be aggregated to be aligned with other events within a visit
      -- The "LOGICAL_OR(is_default_ui)" aggregates the events within a visit to indicate if it's opened in default ui
    LOGICAL_OR(event_category = 'newtab.search' AND event_name = 'issued')
    AND LOGICAL_OR(is_default_ui) AS is_search_issued,
    LOGICAL_OR(event_category = 'newtab.search.ad' AND event_name = 'click')
    AND LOGICAL_OR(is_default_ui) AS is_search_ad_click,
    LOGICAL_OR(
      event_category = 'pocket'
      AND event_name IN ('click', 'dismiss', 'thumb_voting_interaction')
    )
    AND LOGICAL_OR(is_default_ui) AS is_content_interaction,
    LOGICAL_OR(event_category = 'pocket' AND event_name IN ('click'))
    AND LOGICAL_OR(is_default_ui) AS is_content_click,
    LOGICAL_OR(event_category = 'pocket' AND event_name IN ('impression'))
    AND LOGICAL_OR(is_default_ui) AS is_content_impression,
    LOGICAL_OR(
      event_category = 'pocket'
      AND event_name IN ('click', 'dismiss')
      AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
    )
    AND LOGICAL_OR(is_default_ui) AS is_sponsored_content_interaction,
    LOGICAL_OR(
      event_category = 'pocket'
      AND event_name IN ('click')
      AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
    )
    AND LOGICAL_OR(is_default_ui) AS is_sponsored_content_click,
    LOGICAL_OR(
      event_category = 'pocket'
      AND event_name IN ('impression')
      AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
    )
    AND LOGICAL_OR(is_default_ui) AS is_sponsored_content_impression,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(event_category = 'pocket' AND event_name IN ('click')),
      0
    ) AS any_content_click_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(event_category = 'pocket' AND event_name IN ('impression')),
      0
    ) AS any_content_impression_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('click')
        AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS organic_content_click_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('impression')
        AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS organic_content_impression_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('click')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS sponsored_content_click_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('impression')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS sponsored_content_impression_count,
    LOGICAL_OR(event_category = 'topsites' AND event_name IN ('click', 'dismiss'))
    AND LOGICAL_OR(is_default_ui) AS is_topsite_interaction,
    LOGICAL_OR(event_category = 'topsites' AND event_name IN ('click'))
    AND LOGICAL_OR(is_default_ui) AS is_topsite_click,
    LOGICAL_OR(event_category = 'topsites' AND event_name IN ('impression'))
    AND LOGICAL_OR(is_default_ui) AS is_topsite_impression,
    LOGICAL_OR(
      event_category = 'topsites'
      AND event_name IN ('click', 'dismiss')
      AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
    )
    AND LOGICAL_OR(is_default_ui) AS is_sponsored_topsite_interaction,
    LOGICAL_OR(
      event_category = 'topsites'
      AND event_name IN ('click')
      AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
    )
    AND LOGICAL_OR(is_default_ui) AS is_sponsored_topsite_click,
    LOGICAL_OR(
      event_category = 'topsites'
      AND event_name IN ('impression')
      AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
    )
    AND LOGICAL_OR(is_default_ui) AS is_sponsored_topsite_impression,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(event_category = 'topsites' AND event_name IN ('click')),
      0
    ) AS any_topsite_click_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(event_category = 'topsites' AND event_name IN ('impression')),
      0
    ) AS any_topsite_impression_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'topsites'
        AND event_name IN ('click')
        AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS organic_topsite_click_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'topsites'
        AND event_name IN ('impression')
        AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS organic_topsite_impression_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'topsites'
        AND event_name IN ('click')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS sponsored_topsite_click_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'topsites'
        AND event_name IN ('impression')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS sponsored_topsite_impression_count,
    LOGICAL_OR(
      event_category = 'newtab'
      AND event_name IN (
        'weather_change_display',
        'weather_open_provider_url',
        'weather_location_selected',
        'widgets_lists_user_event',
        'widgets_lists_change_display',
        'widgets_timer_change_display',
        'widgets_timer_toggle_notification',
        'widgets_timer_user_event'
      )
    )
    AND LOGICAL_OR(is_default_ui) AS is_widget_interaction,
    LOGICAL_OR(
      event_category = 'newtab'
      AND event_name IN (
        'wallpaper_click',
        'wallpaper_category_click',
        'wallpaper_highlight_cta_click',
        'wallpaper_highlight_dismissed'
      )
    )
    AND LOGICAL_OR(is_default_ui) AS is_wallpaper_interaction,
    LOGICAL_OR(
      event_category = 'newtab'
      AND event_name IN (
        'wallpaper_click',
        'wallpaper_category_click',
        'wallpaper_highlight_cta_click',
        'wallpaper_highlight_dismissed',
        'topic_selection_open',
        'topic_selection_dismiss',
        'topic_selection_topics_saved',
        'sections_block_section',
        'sections_follow_section',
        'sections_unblock_section',
        'sections_unfollow_section',
        'inline_selection_click'
      )
    )
    AND LOGICAL_OR(is_default_ui) AS is_other_interaction,
      -- New fields added Aug 2025
    ANY_VALUE(sample_id) AS sample_id,
    ANY_VALUE(profile_group_id) AS profile_group_id,
    ANY_VALUE(geo_subdivision) AS geo_subdivision,
    LOGICAL_OR(
      event_category = 'pocket'
      AND event_name IN ('impression')
      AND mozfun.map.get_key(event_details, 'section_position') IS NOT NULL
    )
    AND LOGICAL_OR(is_default_ui) AS is_section,
    ANY_VALUE(ping_info.experiments) AS experiments,
    ANY_VALUE(newtab_weather_enabled) AS newtab_weather_enabled,
    ANY_VALUE(default_search_engine) AS default_search_engine,
    ANY_VALUE(default_private_search_engine) AS default_private_search_engine,
    ANY_VALUE(topsite_rows) AS topsite_rows,
    ANY_VALUE(topsite_sponsored_tiles_configured) AS topsite_sponsored_tiles_configured,
    ANY_VALUE(newtab_blocked_sponsors) AS newtab_blocked_sponsors,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('dismiss')
        AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS organic_content_dismissal_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('dismiss')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS sponsored_content_dismissal_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'topsites'
        AND event_name IN ('dismiss')
        AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS organic_topsite_dismissal_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'topsites'
        AND event_name IN ('dismiss')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS sponsored_topsite_dismissal_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_name IN (
          'click',
          'dismiss',
          'issued',
          'thumb_voting_interaction',
          'weather_change_display',
          'weather_open_provider_url',
          'weather_location_selected',
          'widgets_lists_user_event',
          'widgets_lists_change_display',
          'widgets_timer_change_display',
          'widgets_timer_toggle_notification',
          'widgets_timer_user_event',
          'wallpaper_click',
          'wallpaper_category_click',
          'wallpaper_highlight_cta_click',
          'wallpaper_highlight_dismissed',
          'topic_selection_open',
          'topic_selection_dismiss',
          'topic_selection_topics_saved',
          'sections_block_section',
          'sections_follow_section',
          'sections_unblock_section',
          'sections_unfollow_section',
          'inline_selection_click'
        )
      ),
      0
    ) AS any_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(event_category = 'newtab.search' AND event_name IN ('issued')),
      0
    ) AS search_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_name IN (
          'click',
          'dismiss',
          'thumb_voting_interaction',
          'weather_change_display',
          'weather_open_provider_url',
          'weather_location_selected',
          'widgets_lists_user_event',
          'widgets_lists_change_display',
          'widgets_timer_change_display',
          'widgets_timer_toggle_notification',
          'widgets_timer_user_event',
          'wallpaper_click',
          'wallpaper_category_click',
          'wallpaper_highlight_cta_click',
          'wallpaper_highlight_dismissed',
          'topic_selection_open',
          'topic_selection_dismiss',
          'topic_selection_topics_saved',
          'sections_block_section',
          'sections_follow_section',
          'sections_unblock_section',
          'sections_unfollow_section',
          'inline_selection_click'
        )
      ),
      0
    ) AS nonsearch_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('click', 'dismiss', 'thumb_voting_interaction')
      ),
      0
    ) AS any_content_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('click', 'dismiss', 'thumb_voting_interaction')
        AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS organic_content_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('click', 'dismiss')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS sponsored_content_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(event_category = 'topsites' AND event_name IN ('click', 'dismiss')),
      0
    ) AS any_topsite_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'topsites'
        AND event_name IN ('click', 'dismiss')
        AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS organic_topsite_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'topsites'
        AND event_name IN ('click', 'dismiss')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
      ),
      0
    ) AS sponsored_topsite_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('thumb_voting_interaction')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'thumbs_up') AS BOOLEAN)
      ),
      0
    ) AS content_thumbs_up_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'pocket'
        AND event_name IN ('thumb_voting_interaction')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'thumbs_down') AS BOOLEAN)
      ),
      0
    ) AS content_thumbs_down_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(event_category = 'newtab.search.ad' AND event_name IN ('click')),
      0
    ) AS search_ad_click_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(event_category = 'newtab.search.ad' AND event_name IN ('impression')),
      0
    ) AS search_ad_impression_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'newtab'
        AND event_name IN (
          'weather_change_display',
          'weather_open_provider_url',
          'weather_location_selected',
          'widgets_lists_user_event',
          'widgets_lists_change_display',
          'widgets_timer_change_display',
          'widgets_timer_toggle_notification',
          'widgets_timer_user_event'
        )
      ),
      0
    ) AS widget_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'newtab'
        AND event_name IN (
          'weather_impression',
          'widgets_lists_impression',
          'widgets_timer_impression'
        )
      ),
      0
    ) AS widget_impression_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(
        event_category = 'newtab'
        AND event_name IN (
          'wallpaper_click',
          'wallpaper_category_click',
          'wallpaper_highlight_cta_click',
          'wallpaper_highlight_dismissed',
          'topic_selection_open',
          'topic_selection_dismiss',
          'topic_selection_topics_saved',
          'sections_block_section',
          'sections_follow_section',
          'sections_unblock_section',
          'sections_unfollow_section',
          'inline_selection_click'
        )
      ),
      0
    ) AS other_interaction_count,
    IF(
      LOGICAL_OR(is_default_ui),
      COUNTIF(event_category = 'newtab' AND event_name IN ('sections_impression')),
      0
    ) AS other_impression_count,
    MAX(IF(event_category = 'newtab' AND event_name = "closed", event_timestamp, NULL)) - MIN(
      IF(event_category = 'newtab' AND event_name = "opened", event_timestamp, NULL)
    ) AS newtab_visit_duration,
    MIN(
      IF(
        event_category = 'newtab'
        AND event_name = "opened",
        SAFE_CAST(mozfun.map.get_key(event_details, "window_inner_height") AS INT),
        NULL
      )
    ) AS newtab_window_inner_height,
    MIN(
      IF(
        event_category = 'newtab'
        AND event_name = "opened",
        SAFE_CAST(mozfun.map.get_key(event_details, "window_inner_width") AS INT),
        NULL
      )
    ) AS newtab_window_inner_width,
  FROM
    events_unnested
  GROUP BY
    submission_date,
    client_id,
    newtab_visit_id
),
topsites_components AS (
  SELECT
    submission_date,
    client_id,
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
    mozfun.map.get_key(event_details, "tile_id") AS tile_id,
    SAFE_CAST(mozfun.map.get_key(event_details, "position") AS INT64) AS position,
    SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN) AS is_sponsored,
    COUNTIF(event_name = 'impression') AS impression_count,
    COUNTIF(event_name = 'click') AS click_count,
    COUNTIF(event_name = 'dismiss') AS dismissal_count,
    COUNTIF(event_name = 'pin') AS pin_count,
    COUNTIF(event_name = 'unpin') AS unpin_count,
  FROM
    events_unnested
  WHERE
    event_category = 'topsites'
    AND event_name IN ('dismiss', 'click', 'impression', 'pin', 'unpin')
  GROUP BY
    submission_date,
    client_id,
    newtab_visit_id,
    tile_id,
    position,
    is_sponsored
),
topsites_component_summary AS (
  SELECT
    submission_date,
    client_id,
    newtab_visit_id,
    ARRAY_AGG(
      STRUCT(
        tile_id,
        position,
        is_sponsored,
        impression_count,
        click_count,
        dismissal_count,
        pin_count,
        unpin_count
      )
    ) AS topsite_tile_components,
  FROM
    topsites_components
  GROUP BY
    submission_date,
    client_id,
    newtab_visit_id
),
content_components AS (
  SELECT
    submission_date,
    client_id,
    mozfun.map.get_key(event_details, "newtab_visit_id") AS newtab_visit_id,
    SAFE_CAST(mozfun.map.get_key(event_details, "position") AS INT64) AS position,
    SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN) AS is_sponsored,
    SAFE_CAST(mozfun.map.get_key(event_details, 'section_position') AS INT) AS section_position,
    mozfun.map.get_key(event_details, 'format') AS format,
    SAFE_CAST(
      mozfun.map.get_key(event_details, 'is_section_followed') AS BOOLEAN
    ) AS is_section_followed,
    COUNTIF(event_name = 'impression') AS impression_count,
    COUNTIF(event_name = 'click') AS click_count,
    COUNTIF(event_name = 'dismiss') AS dismissal_count,
    COUNTIF(
      event_name = 'thumb_voting_interaction'
      AND SAFE_CAST(mozfun.map.get_key(event_details, 'thumbs_up') AS BOOLEAN)
    ) AS thumbs_up_count,
    COUNTIF(
      event_name = 'thumb_voting_interaction'
      AND SAFE_CAST(mozfun.map.get_key(event_details, 'thumbs_down') AS BOOLEAN)
    ) AS thumbs_down_count,
  FROM
    events_unnested
  WHERE
    event_category = 'pocket'
    AND event_name IN ('dismiss', 'click', 'impression', 'thumb_voting_interaction')
  GROUP BY
    submission_date,
    client_id,
    newtab_visit_id,
    position,
    is_sponsored,
    section_position,
    format,
    is_section_followed
),
content_component_summary AS (
  SELECT
    submission_date,
    client_id,
    newtab_visit_id,
    ARRAY_AGG(
      STRUCT(
        position,
        is_sponsored,
        section_position,
        format,
        is_section_followed,
        impression_count,
        click_count,
        dismissal_count,
        thumbs_up_count,
        thumbs_down_count
      )
    ) AS content_position_components,
  FROM
    content_components
  GROUP BY
    submission_date,
    client_id,
    newtab_visit_id
)
SELECT
  core_visit_metrics.*,
  topsites_component_summary.topsite_tile_components,
  content_component_summary.content_position_components
FROM
  core_visit_metrics
LEFT JOIN
  topsites_component_summary
  USING (submission_date, client_id, newtab_visit_id)
LEFT JOIN
  content_component_summary
  USING (submission_date, client_id, newtab_visit_id)
