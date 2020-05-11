CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fennec_ios_events_v1`
AS
WITH base_events AS (
  SELECT
    *,
    event.f0_ AS timestamp,
    event.f0_ AS event_timestamp,
    event.f1_ AS event_category,
    event.f2_ AS event_method,
    event.f3_ AS event_object,
    event.f4_ AS event_value,
    event.f5_ AS event_map_values,
    metadata.uri.app_version,
    osversion AS os_version,
    metadata.geo.country,
    metadata.geo.city,
    metadata.uri.app_name
  FROM
    `moz-fx-data-shared-prod.telemetry.mobile_event`
  CROSS JOIN
    UNNEST(events) AS event
),
all_events AS (
  SELECT
    submission_timestamp,
    client_id AS device_id,
    (
      created + COALESCE(
        SAFE_CAST(`moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'session_id') AS INT64),
        0
      )
    ) AS session_id,
    CASE
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('foreground'))
      AND (event_object IN ('app'))
    THEN
      'Fennec-iOS - App is foregrounded (session start)'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('background'))
      AND (event_object IN ('app'))
    THEN
      'Fennec-iOS - App is backgrounded (session end)'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('view'))
      AND (event_object IN ('bookmarks-panel'))
      AND (event_value IN ('home-panel-tab-button'))
    THEN
      'Fennec-iOS - View Bookmarks list from Home Panel tab button'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('view'))
      AND (event_object IN ('bookmarks-panel'))
      AND (event_value IN ('app-menu'))
    THEN
      'Fennec-iOS - View Bookmarks list from App Menu'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('add'))
      AND (event_object IN ('bookmark'))
      AND (event_value IN ('page-action-menu'))
    THEN
      'Fennec-iOS - Add Bookmark from Page Action Menu'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('add'))
      AND (event_object IN ('bookmark'))
      AND (event_value IN ('share-menu'))
    THEN
      'Fennec-iOS - Add Bookmark from Share Menu'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('add'))
      AND (event_object IN ('bookmark'))
      AND (event_value IN ('activity-stream'))
    THEN
      'Fennec-iOS - Add Bookmark from Activity Stream context menu'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('delete'))
      AND (event_object IN ('bookmark'))
      AND (event_value IN ('page-action-menu'))
    THEN
      'Fennec-iOS - Delete Bookmark from Page Action Menu'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('delete'))
      AND (event_object IN ('bookmark'))
      AND (event_value IN ('activity-stream'))
    THEN
      'Fennec-iOS - Delete Bookmark from Activity Stream context menu'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('delete'))
      AND (event_object IN ('bookmark'))
      AND (event_value IN ('bookmarks-panel'))
    THEN
      'Fennec-iOS - Delete Bookmark from Home Panel via long-press'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('delete'))
      AND (event_object IN ('bookmark'))
      AND (event_value IN ('bookmarks-panel'))
    THEN
      'Fennec-iOS - Delete Bookmark from Home Panel via swipe'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('open'))
      AND (event_object IN ('bookmark'))
      AND (event_value IN ('awesomebar-results'))
    THEN
      'Fennec-iOS - Open Bookmark from Awesomebar search results'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('open'))
      AND (event_object IN ('bookmark'))
      AND (event_value IN ('bookmarks-panel'))
    THEN
      'Fennec-iOS - Open Bookmark from Home Panel'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('reader-mode-open-button'))
    THEN
      'Fennec-iOS - Open Reader Mode'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('reader-mode-close-button'))
    THEN
      'Fennec-iOS - Leave Reader Mode'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('add'))
      AND (event_object IN ('reading-list-item'))
      AND (event_value IN ('reader-mode-toolbar'))
    THEN
      'Fennec-iOS - Add item to Reading List from Toolbar'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('add'))
      AND (event_object IN ('reading-list-item'))
      AND (event_value IN ('share-extension'))
    THEN
      'Fennec-iOS - Add item to Reading List from Share Extension'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('add'))
      AND (event_object IN ('reading-list-item'))
      AND (event_value IN ('page-action-menu'))
    THEN
      'Fennec-iOS - Add item to Reading List from Page Action Menu'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('open'))
      AND (event_object IN ('reading-list-item'))
    THEN
      'Fennec-iOS - Open item from Reading List'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('delete'))
      AND (event_object IN ('reading-list-item'))
      AND (event_value IN ('reader-mode-toolbar'))
    THEN
      'Fennec-iOS - Delete item from Reading List from Toolbar'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('delete'))
      AND (event_object IN ('reading-list-item'))
      AND (event_value IN ('reading-list-panel'))
    THEN
      'Fennec-iOS - Delete item from Reading List from Home Panel'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('reading-list-item'))
      AND (event_value IN ('mark-as-read'))
    THEN
      'Fennec-iOS - Mark Item As Read'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('reading-list-item'))
      AND (event_value IN ('mark-as-unread'))
    THEN
      'Fennec-iOS - Mark Item As Unread'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('change'))
      AND (event_object IN ('setting'))
    THEN
      'Fennec-iOS - Setting changed'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('scan'))
      AND (event_object IN ('qr-code-url'))
    THEN
      'Fennec-iOS - URL-based QR code scanned'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('scan'))
      AND (event_object IN ('qr-code-text'))
    THEN
      'Fennec-iOS - Non-URL-based QR code scanned'
    WHEN
      (event_category IN ('app-extension-action'))
      AND (event_method IN ('send-to-device'))
      AND (event_object IN ('url'))
    THEN
      'Fennec-iOS - Send to device tapped'
    WHEN
      (event_category IN ('app-extension-action'))
      AND (event_method IN ('application-open-url'))
      AND (event_object IN ('url'))
    THEN
      'Fennec-iOS - Open in Firefox tapped (URL)'
    WHEN
      (event_category IN ('app-extension-action'))
      AND (event_method IN ('application-open-url'))
      AND (event_object IN ('searchText'))
    THEN
      'Fennec-iOS - Open in Firefox tapped (search text)'
    WHEN
      (event_category IN ('app-extension-action'))
      AND (event_method IN ('bookmark-this-page'))
      AND (event_object IN ('url'))
    THEN
      'Fennec-iOS - Bookmark this page tapped'
    WHEN
      (event_category IN ('app-extension-action'))
      AND (event_method IN ('add-to-reading-list'))
      AND (event_object IN ('url'))
    THEN
      'Fennec-iOS - Add to reading list tapped'
    WHEN
      (event_category IN ('app-extension-action'))
      AND (event_method IN ('load-in-background'))
      AND (event_object IN ('url'))
    THEN
      'Fennec-iOS - Load in Background tapped'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('website-data-button'))
    THEN
      'Fennec-iOS - Tap Website Data in Data Management Menu'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('search-website-data'))
    THEN
      'Fennec-iOS - Tap on the Search Bar in Website Data'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('clear-website-data-button'))
    THEN
      'Fennec-iOS - Tap on \'Clear All Website Data\' button in Website Data'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('show-top-sites-button'))
    THEN
      'Fennec-iOS - Tap Top Sites button in New Tab Settings'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('show-blank-page-button'))
    THEN
      'Fennec-iOS - Tap Blank Page button in New Tab Settings'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('show-bookmarks-button'))
    THEN
      'Fennec-iOS - Tap Bookmarks button in New Tab Settings'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('show-history-button'))
    THEN
      'Fennec-iOS - Tap History button in New Tab Settings'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('tap'))
      AND (event_object IN ('show-homepage-button'))
    THEN
      'Fennec-iOS - Tap Homepage button in New Tab Settings'
    WHEN
      (event_category IN ('prompt'))
      AND (event_method IN ('translate'))
      AND (event_object IN ('tab'))
    THEN
      'Fennec-iOS - Prompt to translate tab to user\'s native language'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('translate'))
      AND (event_object IN ('tab'))
    THEN
      'Fennec-iOS - Accept or decline offer to translate tab'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('press'))
      AND (event_object IN ('dismissed-onboarding'))
    THEN
      'Fennec-iOS - Dismiss onboarding screen'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('press'))
      AND (event_object IN ('dismissed-onboarding-email-login'))
    THEN
      'Fennec-iOS - Dismiss onboarding screen on email login card'
    WHEN
      (event_category IN ('action'))
      AND (event_method IN ('press'))
      AND (event_object IN ('dismissed-onboarding-sign-up'))
    THEN
      'Fennec-iOS - Dismiss onboarding screen on sign up card'
    WHEN
      NOT ( -- events above are legacy definitions, moved from the previous Spark job. Following statement captures all new, non-ignored events
        (event_category = 'action' AND event_method = 'add' AND event_object = 'tab')
        OR (event_category = 'action' AND event_method = 'delete' AND event_object = 'tab')
      )
    THEN
      CONCAT('Fennec-iOS - ', event_category, '.', event_method, '.', event_object)
    END
    AS event_name,
    event_timestamp AS timestamp,
    (event_timestamp + created) AS time,
    app_version,
    os AS os_name,
    os_version,
    country,
    city,
    (
      SELECT
        ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"'))
      FROM
        UNNEST(event_map_values)
    ) AS event_props_1,
    event_map_values,
    event_object,
    event_value,
    event_method,
    event_category,
    created,
    settings
  FROM
    base_events
  WHERE
    app_name IN ('Fennec')
    AND os IN ('iOS')
  -- bug 1636231 - disable background events in ios amplitude
    AND event_method != 'background'
),
all_events_with_insert_ids AS (
  SELECT
    * EXCEPT (event_category, created),
    CONCAT(
      device_id,
      "-",
      CAST(created AS STRING),
      "-",
      SPLIT(event_name, " - ")[OFFSET(1)],
      "-",
      CAST(timestamp AS STRING),
      "-",
      event_category,
      "-",
      event_method,
      "-",
      event_object
    ) AS insert_id,
    event_name AS event_type
  FROM
    all_events
  WHERE
    event_name IS NOT NULL
),
extra_props AS (
  SELECT
    * EXCEPT (event_map_values, event_object, event_value, event_method, event_name),
    (
      SELECT
        ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"'))
      FROM
        (
          SELECT
            'gesture' AS key,
            CASE
            WHEN
              event_name = 'Fennec-iOS - Delete Bookmark from Home Panel via long-press'
            THEN
              'long-press'
            WHEN
              event_name = 'Fennec-iOS - Delete Bookmark from Home Panel via swipe'
            THEN
              'swipe'
            END
            AS value
          UNION ALL
          SELECT
            'setting' AS key,
            CASE
            WHEN
              event_name = 'Fennec-iOS - Setting changed'
            THEN
              event_value
            END
            AS value
          UNION ALL
          SELECT
            'to' AS key,
            CASE
            WHEN
              event_name = 'Fennec-iOS - Setting changed'
            THEN
              `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'to')
            WHEN
              event_name = 'Fennec-iOS - Prompt to translate tab to user\'s native language'
            THEN
              `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'to')
            WHEN
              event_name = 'Fennec-iOS - Accept or decline offer to translate tab'
            THEN
              `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'to')
            END
            AS value
          UNION ALL
          SELECT
            'from' AS key,
            CASE
            WHEN
              event_name = 'Fennec-iOS - Prompt to translate tab to user\'s native language'
            THEN
              `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'from')
            WHEN
              event_name = 'Fennec-iOS - Accept or decline offer to translate tab'
            THEN
              `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'from')
            END
            AS value
          UNION ALL
          SELECT
            'action' AS key,
            CASE
            WHEN
              event_name = 'Fennec-iOS - Accept or decline offer to translate tab'
            THEN
              `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'action')
            END
            AS value
          UNION ALL
          SELECT
            'slide-num' AS key,
            CASE
            WHEN
              event_name = 'Fennec-iOS - Dismiss onboarding screen'
            THEN
              `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'slide-num')
            WHEN
              event_name = 'Fennec-iOS - Dismiss onboarding screen on email login card'
            THEN
              `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'slide-num')
            WHEN
              event_name = 'Fennec-iOS - Dismiss onboarding screen on sign up card'
            THEN
              `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'slide-num')
            END
            AS value
          UNION ALL
          SELECT
            'event_value' AS key,
            event_value AS value
        )
      WHERE
        VALUE IS NOT NULL
    ) AS event_props_2,
    ARRAY_CONCAT(
      ARRAY<STRING>[],
      (
        SELECT
          ARRAY_AGG(
            CASE
            WHEN
              key = 'defaultSearchEngine'
            THEN
              CONCAT('"', 'pref_default_search_engine', '":"', CAST(value AS STRING), '"')
            WHEN
              key = 'prefKeyAutomaticSliderValue'
            THEN
              CONCAT('"', 'pref_automatic_slider_value', '":"', CAST(value AS STRING), '"')
            WHEN
              key = 'prefKeyAutomaticSwitchOnOff'
            THEN
              CONCAT('"', 'pref_automatic_switch_on_off', '":"', CAST(value AS STRING), '"')
            WHEN
              key = 'prefKeyThemeName'
            THEN
              CONCAT('"', 'pref_theme_name', '":"', CAST(value AS STRING), '"')
            WHEN
              key = 'profile.ASBookmarkHighlightsVisible'
            THEN
              CONCAT(
                '"',
                'pref_activity_stream_bookmark_highlights_visible',
                '":',
                CAST(SAFE_CAST(value AS BOOLEAN) AS STRING)
              )
            WHEN
              key = 'profile.ASPocketStoriesVisible'
            THEN
              CONCAT(
                '"',
                'pref_activity_stream_pocket_stories_visible',
                '":',
                CAST(SAFE_CAST(value AS BOOLEAN) AS STRING)
              )
            WHEN
              key = 'profile.ASRecentHighlightsVisible'
            THEN
              CONCAT(
                '"',
                'pref_activity_stream_recent_highlights_visible',
                '":',
                CAST(SAFE_CAST(value AS BOOLEAN) AS STRING)
              )
            WHEN
              key = 'profile.blockPopups'
            THEN
              CONCAT('"', 'pref_block_popups', '":', CAST(SAFE_CAST(value AS BOOLEAN) AS STRING))
            WHEN
              key = 'profile.prefkey.trackingprotection.enabled'
            THEN
              CONCAT('"', 'pref_tracking_protection_enabled', '":"', CAST(value AS STRING), '"')
            WHEN
              key = 'profile.prefkey.trackingprotection.normalbrowsing'
            THEN
              CONCAT(
                '"',
                'pref_tracking_protection_normal_browsing',
                '":"',
                CAST(value AS STRING),
                '"'
              )
            WHEN
              key = 'profile.prefkey.trackingprotection.privatebrowsing'
            THEN
              CONCAT(
                '"',
                'pref_tracking_protection_private_browsing',
                '":"',
                CAST(value AS STRING),
                '"'
              )
            WHEN
              key = 'profile.prefkey.trackingprotection.strength'
            THEN
              CONCAT('"', 'pref_tracking_protection_strength', '":"', CAST(value AS STRING), '"')
            WHEN
              key = 'profile.saveLogins'
            THEN
              CONCAT('"', 'pref_save_logins', '":', CAST(SAFE_CAST(value AS BOOLEAN) AS STRING))
            WHEN
              key = 'profile.settings.closePrivateTabs'
            THEN
              CONCAT(
                '"',
                'pref_settings_close_private_tabs',
                '":',
                CAST(SAFE_CAST(value AS BOOLEAN) AS STRING)
              )
            WHEN
              key = 'profile.show-translation'
            THEN
              CONCAT('"', 'pref_show_translation', '":"', CAST(value AS STRING), '"')
            WHEN
              key = 'profile.showClipboardBar'
            THEN
              CONCAT(
                '"',
                'pref_show_clipboard_bar',
                '":',
                CAST(SAFE_CAST(value AS BOOLEAN) AS STRING)
              )
            WHEN
              key = 'windowHeight'
            THEN
              CONCAT('"', 'pref_window_height', '":"', CAST(value AS STRING), '"')
            WHEN
              key = 'windowWidth'
            THEN
              CONCAT('"', 'pref_window_width', '":"', CAST(value AS STRING), '"')
            END
            IGNORE NULLS
          )
        FROM
          UNNEST(SETTINGS)
      )
    ) AS user_props
  FROM
    all_events_with_insert_ids
)
SELECT
  * EXCEPT (event_props_1, event_props_2, user_props, settings),
  CONCAT(
    '{',
    ARRAY_TO_STRING(
      (
        SELECT
          ARRAY_AGG(DISTINCT e)
        FROM
          UNNEST(ARRAY_CONCAT(IFNULL(event_props_1, []), IFNULL(event_props_2, []))) AS e
      ),
      ","
    ),
    '}'
  ) AS event_properties,
  CONCAT('{', ARRAY_TO_STRING(user_props, ","), '}') AS user_properties
FROM
  extra_props
