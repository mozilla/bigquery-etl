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
    client_info.app_display_version AS browser_version,
    normalized_country_code AS country,
    metrics.string.newtab_homepage_category AS newtab_homepage_category,
    metrics.string.newtab_newtab_category AS newtab_newtab_category,
    metrics.boolean.pocket_enabled AS organic_content_enabled,
    metrics.boolean.pocket_sponsored_stories_enabled AS sponsored_content_enabled,
    metrics.boolean.topsites_sponsored_enabled AS sponsored_topsites_enabled,
    metrics.boolean.topsites_enabled AS organic_topsites_enabled,
    metrics.boolean.newtab_search_enabled AS newtab_search_enabled,
    category AS event_category,
    name AS event_name,
    extra AS event_details,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
    -- selecting the events we need to compute on
    AND (
      (category = 'newtab.search' AND name IN ('issued'))
      OR (category = 'newtab.search.ad' AND name IN ('click'))
      OR (
        category = 'pocket'
        AND name IN (
          'click',
          'impression',
          'dismiss',
          'thumb_voting_interaction',
          'thumb_voting_interaction'
        )
      )
      OR (category = 'topsites' AND name IN ('click', 'impression', 'dismiss'))
      OR (
        category = 'newtab'
        AND name IN (
          'opened',
          -- weather
          'weather_change_display',
          'weather_open_provider_url',
          'weather_location_selected',
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
          'inline_selection_click'
        )
      )
    )
)
SELECT
  submission_date,
  client_id,
  mozfun.map.get_key(event_details, 'newtab_visit_id') AS newtab_visit_id,
  -- ANY_VALUE: because we do not expect those fields to differ within a visit
  ANY_VALUE(app_version) AS app_version,
  ANY_VALUE(os) AS os,
  ANY_VALUE(channel) AS channel,
  ANY_VALUE(locale) AS locale,
  ANY_VALUE(browser_version) AS browser_version,
  ANY_VALUE(country) AS country,
  ANY_VALUE(newtab_homepage_category) AS newtab_homepage_category,
  ANY_VALUE(newtab_newtab_category) AS newtab_newtab_category,
  ANY_VALUE(organic_content_enabled) AS organic_content_enabled,
  ANY_VALUE(sponsored_content_enabled) AS sponsored_content_enabled,
  ANY_VALUE(sponsored_topsites_enabled) AS sponsored_topsites_enabled,
  ANY_VALUE(organic_topsites_enabled) AS organic_topsites_enabled,
  ANY_VALUE(newtab_search_enabled) AS newtab_search_enabled,
  LOGICAL_OR(
    event_category = 'newtab'
    AND event_name = 'opened'
    AND (
      (
        mozfun.map.get_key(event_details, 'source') = 'about:home'
        AND newtab_homepage_category = 'enabled'
      )
      OR (
        mozfun.map.get_key(event_details, 'source') = 'about:newtab'
        AND newtab_newtab_category = 'enabled'
      )
    )
  ) AS is_default_ui,
  LOGICAL_OR(event_category = 'newtab' AND event_name = 'opened') AS is_newtab_opened,
  LOGICAL_OR(event_category = 'newtab.search' AND event_name = 'issued') AS is_search_issued,
  LOGICAL_OR(event_category = 'newtab.search.ad' AND event_name = 'click') AS is_search_ad_click,
  LOGICAL_OR(
    event_category = 'pocket'
    AND event_name IN ('click', 'dismiss', 'thumb_voting_interaction', 'thumb_voting_interaction')
  ) AS is_content_interaction,
  LOGICAL_OR(event_category = 'pocket' AND event_name IN ('click')) AS is_content_click,
  LOGICAL_OR(event_category = 'pocket' AND event_name IN ('impression')) AS is_content_impression,
  LOGICAL_OR(
    event_category = 'pocket'
    AND event_name IN ('click', 'dismiss')
    AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS is_sponsored_content_interaction,
  LOGICAL_OR(
    event_category = 'pocket'
    AND event_name IN ('click')
    AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS is_sponsored_content_click,
  LOGICAL_OR(
    event_category = 'pocket'
    AND event_name IN ('impression')
    AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS is_sponsored_content_impression,
  COUNTIF(event_category = 'pocket' AND event_name IN ('click')) AS any_content_click_count,
  COUNTIF(
    event_category = 'pocket'
    AND event_name IN ('impression')
  ) AS any_content_impression_count,
  COUNTIF(
    event_category = 'pocket'
    AND event_name IN ('click')
    AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS organic_content_click_count,
  COUNTIF(
    event_category = 'pocket'
    AND event_name IN ('impression')
    AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS organic_content_impression_count,
  COUNTIF(
    event_category = 'pocket'
    AND event_name IN ('click')
    AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS sponsored_content_click_count,
  COUNTIF(
    event_category = 'pocket'
    AND event_name IN ('impression')
    AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS sponsored_content_impression_count,
  LOGICAL_OR(
    event_category = 'topsites'
    AND event_name IN ('click', 'dismiss')
  ) AS is_topsites_interaction,
  LOGICAL_OR(event_category = 'topsites' AND event_name IN ('click')) AS is_topsites_click,
  LOGICAL_OR(
    event_category = 'topsites'
    AND event_name IN ('impression')
  ) AS is_topsites_impression,
  LOGICAL_OR(
    event_category = 'topsites'
    AND event_name IN ('click', 'dismiss')
    AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS is_sponsored_topsites_interaction,
  LOGICAL_OR(
    event_category = 'topsites'
    AND event_name IN ('click')
    AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS is_sponsored_topsites_click,
  LOGICAL_OR(
    event_category = 'topsites'
    AND event_name IN ('impression')
    AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS is_sponsored_topsites_impression,
  COUNTIF(event_category = 'topsites' AND event_name IN ('click')) AS any_topsites_click_count,
  COUNTIF(
    event_category = 'topsites'
    AND event_name IN ('impression')
  ) AS any_topsites_impression_count,
  COUNTIF(
    event_category = 'topsites'
    AND event_name IN ('click')
    AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS organic_topsites_click_count,
  COUNTIF(
    event_category = 'topsites'
    AND event_name IN ('impression')
    AND NOT SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS organic_topsites_impression_count,
  COUNTIF(
    event_category = 'topsites'
    AND event_name IN ('click')
    AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS sponsored_topsites_click_count,
  COUNTIF(
    event_category = 'topsites'
    AND event_name IN ('impression')
    AND SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN)
  ) AS sponsored_topsites_impression_count,
  LOGICAL_OR(
    event_category = 'newtab'
    AND event_name IN (
      'weather_change_display',
      'weather_open_provider_url',
      'weather_location_selected'
    )
  ) AS is_widget_interaction,
  LOGICAL_OR(
    event_category = 'newtab'
    AND event_name IN (
      'wallpaper_click',
      'wallpaper_category_click',
      'wallpaper_highlight_cta_click',
      'wallpaper_highlight_dismissed'
    )
  ) AS is_wallpaper_interaction,
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
  ) AS is_other_interaction,
FROM
  events_unnested
GROUP BY
  submission_date,
  client_id,
  newtab_visit_id
