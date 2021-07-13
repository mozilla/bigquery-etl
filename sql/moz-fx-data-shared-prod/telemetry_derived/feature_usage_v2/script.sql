WITH user_type AS (
  SELECT
    cls.client_id,
    cls.submission_date,
    cls.activity_segments_v1,
    cls.is_allweek_regular_v1,
    cls.is_weekday_regular_v1,
    cls.is_core_active_v1,
    cls.days_since_first_seen,
    cls.days_since_seen,
    cls.new_profile_7_day_activated_v1,
    cls.new_profile_14_day_activated_v1,
    cls.new_profile_21_day_activated_v1,
    cls.first_seen_date,
    cls.days_since_created_profile,
    cls.profile_creation_date,
    cls.country,
    cls.scalar_parent_os_environment_is_taskbar_pinned,
    cls.scalar_parent_os_environment_launched_via_desktop,
    cls.scalar_parent_os_environment_launched_via_other,
    cls.scalar_parent_os_environment_launched_via_taskbar,
    cls.scalar_parent_os_environment_launched_via_start_menu,
    cls.scalar_parent_os_environment_launched_via_other_shortcut,
    cls.os,
    cls.normalized_os_version,
    cls.app_version,
    (cls.attribution.campaign IS NOT NULL)
    OR (cls.attribution.source IS NOT NULL) AS attributed,
    cls.is_default_browser,
    cls.sync_count_desktop_mean,
    cls.sync_count_mobile_mean,
    cls.active_hours_sum,
    cls.subsession_hours_sum scalar_parent_browser_engagement_total_uri_count_sum,
    ad_clicks_count_all,
    scalar_parent_browser_engagement_tab_open_event_count_sum,
    scalar_parent_browser_engagement_window_open_event_count_sum,
    scalar_parent_browser_engagement_unique_domains_count_max,
    scalar_parent_browser_engagement_unique_domains_count_mean,
    scalar_parent_browser_engagement_max_concurrent_tab_count_max,
    scalar_parent_browser_engagement_max_concurrent_window_count_max,
    search_count_abouthome,
    search_count_all,
    search_count_contextmenu,
    search_count_newtab,
    search_count_organic,
    search_count_searchbar,
    search_count_system,
    search_count_tagged_follow_on,
    search_count_tagged_sap,
    search_count_urlbar,
    search_with_ads_count_all
  FROM
    telemetry.clients_last_seen cls
  WHERE
    cls.submission_date = @submission_date
    AND cls.sample_id = 0
    AND cls.normalized_channel = 'release'
    AND days_since_seen = 0
),
main AS (
  SELECT
    client_id,
    MOD(ABS(FARM_FINGERPRINT(client_id)), 100) AS subsample_id,
    DATE(submission_timestamp) AS submission_date,
    LOGICAL_OR(COALESCE(environment.system.gfx.headless, FALSE)) AS is_headless,
    SUM(
      COALESCE(
        CAST(
          JSON_EXTRACT_SCALAR(
            payload.processes.content.histograms.video_play_time_ms,
            '$.sum'
          ) AS int64
        ),
        0
      )
    ) AS video_play_time_ms,
    SUM(
      COALESCE(
        CAST(
          JSON_EXTRACT_SCALAR(
            payload.processes.content.histograms.video_encrypted_play_time_ms,
            '$.sum'
          ) AS int64
        ),
        0
      )
    ) AS video_encrypted_play_time_ms,
    SUM(
      COALESCE(
        CAST(
          JSON_EXTRACT_SCALAR(
            payload.processes.content.histograms.pdf_viewer_time_to_view_ms,
            '$.sum'
          ) AS int64
        ),
        0
      )
    ) AS pdf_viewer_time_to_view_ms_content,
    SUM(
      COALESCE(
        CAST(
          JSON_EXTRACT_SCALAR(
            payload.histograms.fx_picture_in_picture_window_open_duration,
            '$.sum'
          ) AS int64
        ),
        0
      )
    ) AS pip_window_open_duration,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(
              mozfun.hist.extract(payload.processes.content.histograms.video_play_time_ms).values
            )
        ),
        0
      )
    ) AS video_play_time_ms_count,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(
              mozfun.hist.extract(
                payload.processes.content.histograms.video_encrypted_play_time_ms
              ).values
            )
        ),
        0
      )
    ) AS video_encrypted_play_time_ms_count,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(
              mozfun.hist.extract(
                payload.processes.content.histograms.pdf_viewer_time_to_view_ms
              ).values
            )
        ),
        0
      )
    ) AS pdf_viewer_time_to_view_ms_content_count,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(
              mozfun.hist.extract(
                payload.histograms.fx_picture_in_picture_window_open_duration
              ).values
            )
        ),
        0
      )
    ) AS pip_window_open_duration_count,
    SUM(
      COALESCE(
        CAST(JSON_EXTRACT_SCALAR(payload.histograms.pdf_viewer_document_size_kb, '$.sum') AS int64),
        0
      )
    ) AS pdf_viewer_doc_size_kb,
    SUM(
      COALESCE(
        CAST(
          JSON_EXTRACT_SCALAR(
            payload.processes.content.histograms.pdf_viewer_document_size_kb,
            '$.sum'
          ) AS int64
        ),
        0
      )
    ) AS pdf_viewer_doc_size_kb_content,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(mozfun.hist.extract(payload.histograms.pdf_viewer_document_size_kb).values)
        ),
        0
      )
    ) AS pdf_viewer_doc_size_kb_count,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(
              mozfun.hist.extract(
                payload.processes.content.histograms.pdf_viewer_document_size_kb
              ).values
            )
        ),
        0
      )
    ) AS pdf_viewer_doc_size_kb_content_count,
    COUNTIF(COALESCE(environment.services.account_enabled, FALSE)) > 0 AS sync_signed_in,
    COUNTIF(
      COALESCE(
        payload.processes.parent.scalars.formautofill_credit_cards_autofill_profiles_count IS NOT NULL,
        FALSE
      )
    ) > 0 AS ccards_saved,
    COUNTIF(
      COALESCE(payload.processes.parent.scalars.dom_parentprocess_private_window_used, FALSE)
    ) > 0 AS pbm_used,
    SUM(
      COALESCE(ARRAY_LENGTH(payload.processes.parent.keyed_scalars.sidebar_opened), 0)
    ) AS unique_sidebars_accessed_count,
    SUM(
      COALESCE(
        (SELECT SUM(value) FROM UNNEST(payload.processes.parent.keyed_scalars.sidebar_opened)),
        0
      )
    ) AS sidebars_accessed_total,
    SUM(
      COALESCE(ARRAY_LENGTH(payload.processes.parent.keyed_scalars.urlbar_picked_history), 0)
    ) AS unique_history_urlbar_indices_picked_count,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(payload.processes.parent.keyed_scalars.urlbar_picked_history)
        ),
        0
      )
    ) AS history_urlbar_picked_total,
    SUM(
      COALESCE(ARRAY_LENGTH(payload.processes.parent.keyed_scalars.urlbar_picked_remotetab), 0)
    ) AS unique_remotetab_indices_picked_count,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(payload.processes.parent.keyed_scalars.urlbar_picked_remotetab)
        ),
        0
      )
    ) AS remotetab_picked_total,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(
              payload.processes.parent.keyed_scalars.browser_engagement_navigation_about_newtab
            )
        ),
        0
      )
    ) AS uris_from_newtab,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(payload.processes.parent.keyed_scalars.browser_engagement_navigation_searchbar)
        ),
        0
      )
    ) AS uris_from_searchbar,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(payload.processes.parent.keyed_scalars.browser_engagement_navigation_urlbar)
        ),
        0
      )
    ) AS uris_from_urlbar,
    SUM(
      COALESCE(
        mozfun.map.get_key(
          mozfun.hist.extract(payload.histograms.fx_urlbar_selected_result_type_2).values,
          2
        ),
        0
      )
    ) AS nav_history_urlbar,
    SUM(
      COALESCE(
        mozfun.map.get_key(
          mozfun.hist.extract(payload.histograms.fx_urlbar_selected_result_type_2).values,
          0
        ),
        0
      )
    ) AS nav_autocomplete_urlbar,
    SUM(
      COALESCE(
        mozfun.map.get_key(
          mozfun.hist.extract(payload.histograms.fx_urlbar_selected_result_type_2).values,
          8
        ),
        0
      )
    ) AS nav_visiturl_urlbar,
    SUM(
      COALESCE(
        mozfun.map.get_key(
          mozfun.hist.extract(payload.histograms.fx_urlbar_selected_result_type_2).values,
          5
        ),
        0
      )
    ) AS nav_searchsuggestion_urlbar,
    SUM(
      COALESCE(
        mozfun.map.get_key(
          mozfun.hist.extract(payload.histograms.fx_urlbar_selected_result_type_2).values,
          13
        ),
        0
      )
    ) AS nav_topsite_urlbar,
    MAX(
      COALESCE(
        CAST(JSON_EXTRACT_SCALAR(payload.histograms.pwmgr_num_saved_passwords, '$.sum') AS int64),
        0
      )
    ) AS num_passwords_saved,
    SUM(
      COALESCE(
        ARRAY_LENGTH(
          payload.processes.parent.keyed_scalars.browser_ui_interaction_preferences_pane_general
        ),
        0
      )
    ) AS unique_preferences_accessed_count,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(
              payload.processes.parent.keyed_scalars.browser_ui_interaction_preferences_pane_general
            )
        ),
        0
      )
    ) AS preferences_accessed_total,
    SUM(
      COALESCE(
        ARRAY_LENGTH(payload.processes.parent.keyed_scalars.browser_ui_interaction_bookmarks_bar),
        0
      )
    ) AS unique_bookmarks_bar_accessed_count,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(payload.processes.parent.keyed_scalars.browser_ui_interaction_bookmarks_bar)
        ),
        0
      )
    ) AS bookmarks_bar_accessed_total,
    SUM(
      COALESCE(
        ARRAY_LENGTH(payload.processes.parent.keyed_scalars.browser_ui_interaction_keyboard),
        0
      )
    ) AS unique_keyboard_shortcut_count,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(payload.processes.parent.keyed_scalars.browser_ui_interaction_keyboard)
        ),
        0
      )
    ) AS keyboard_shortcut_total,
  FROM
    telemetry.main_1pct
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id = 0
    AND normalized_channel = 'release'
  GROUP BY
    submission_date,
    client_id
),
events AS (
  SELECT
    e.client_id,
    e.submission_date,
    COALESCE(
      COUNTIF(event_category = 'pictureinpicture' AND event_method = 'create'),
      0
    ) AS pip_count,
    COALESCE(
      COUNTIF(event_category = 'security.ui.protections' AND event_object = 'protection_report'),
      0
    ) AS viewed_protection_report_count,
    COUNTIF(
      event_category = 'security.ui.protectionspopup'
      AND event_object = 'etp_toggle_off'
    ) AS etp_toggle_off,
    COUNTIF(
      event_category = 'security.ui.protectionspopup'
      AND event_object = 'etp_toggle_on'
    ) AS etp_toggle_on,
    COUNTIF(
      event_category = 'security.ui.protectionspopup'
      AND event_object = 'protections_popup'
    ) AS protections_popup,
    COALESCE(
      COUNTIF(
        event_category = 'creditcard'
        AND event_object = 'cc_form'
        AND event_method = 'filled'
      ),
      0
    ) AS ccard_filled,
    COALESCE(
      COUNTIF(
        event_category = 'creditcard'
        AND event_object = 'capture_doorhanger'
        AND event_method = 'save'
      ),
      0
    ) AS ccard_saved,
    COALESCE(
      COUNTIF(
        event_method = 'install'
        AND event_category = 'addonsManager'
        AND event_object = 'extension'
      ),
      0
    ) AS installed_extension,
    COALESCE(
      COUNTIF(
        event_method = 'install'
        AND event_category = 'addonsManager'
        AND event_object = 'theme'
      ),
      0
    ) AS installed_theme,
    COALESCE(
      COUNTIF(
        event_method = 'install'
        AND event_category = 'addonsManager'
        AND event_object IN ('dictionary', 'locale')
      ),
      0
    ) AS installed_l10n,
    COALESCE(COUNTIF(event_method = 'saved_login_used'), 0) AS used_stored_pw,
    COALESCE(
      COUNTIF(
        event_category = 'pwmgr'
        AND event_object IN ('form_login', 'form_password', 'auth_login', 'prompt_login')
      ),
      0
    ) AS password_filled,
    COALESCE(
      COUNTIF(
        event_category = 'pwmgr'
        AND event_method = 'doorhanger_submitted'
        AND event_object = 'save'
      ),
      0
    ) AS password_saved,
    COALESCE(
      COUNTIF(event_category = 'pwmgr' AND event_method = 'open_management'),
      0
    ) AS pwmgr_opened,
    COALESCE(
      COUNTIF(event_category = 'pwmgr' AND event_method IN ('copy', 'show')),
      0
    ) AS pwmgr_copy_or_show_info,
    COALESCE(
      COUNTIF(
        event_category = 'pwmgr'
        AND event_method IN ('dismiss_breach_alert', 'learn_more_breach')
      ),
      0
    ) AS pwmgr_interacted_breach,
    COALESCE(
      COUNTIF(event_object = 'generatedpassword' AND event_method = 'autocomplete_field'),
      0
    ) AS generated_password,
    COALESCE(COUNTIF(event_category = 'fxa' AND event_method = 'connect'), 0) AS fxa_connect,
    COALESCE(
      COUNTIF(
        event_category = 'normandy'
        AND event_object IN (
          "preference_study",
          "addon_study",
          "preference_rollout",
          "addon_rollout"
        )
      ),
      0
    ) AS normandy_enrolled,
    COALESCE(COUNTIF(event_category = 'downloads'), 0) AS downloads,
    COALESCE(
      COUNTIF(event_category = 'downloads' AND event_string_value = 'pdf'),
      0
    ) AS pdf_downloads,
    COALESCE(
      COUNTIF(event_category = 'downloads' AND event_string_value IN ('jpg', 'jpeg', 'png', 'gif')),
      0
    ) AS image_downloads,
    COALESCE(
      COUNTIF(event_category = 'downloads' AND event_string_value IN ('mp4', 'mp3', 'wav', 'mov')),
      0
    ) AS media_downloads,
    COALESCE(
      COUNTIF(
        event_category = 'downloads'
        AND event_string_value IN ('xlsx', 'docx', 'pptx', 'xls', 'ppt', 'doc')
      ),
      0
    ) AS msoffice_downloads,
    COALESCE(
      COUNTIF(event_category = 'activity_stream' AND event_object IN ('CLICK')),
      0
    ) AS newtab_click,
    COALESCE(
      COUNTIF(event_category = 'activity_stream' AND event_object IN ('BOOKMARK_ADD')),
      0
    ) AS bookmark_added_from_newtab,
    COALESCE(
      COUNTIF(event_category = 'activity_stream' AND event_object IN ('SAVE_TO_POCKET')),
      0
    ) AS saved_to_pocket_from_newtab,
    COALESCE(
      COUNTIF(event_category = 'activity_stream' AND event_object IN ('OPEN_NEWTAB_PREFS')),
      0
    ) AS newtab_prefs_opened
  FROM
    telemetry.events e
  WHERE
    e.submission_date = @submission_date
    AND e.sample_id = 0
    AND e.normalized_channel = 'release'
  GROUP BY
    client_id,
    submission_date
),
activity_stream_events AS (
  SELECT
    client_id,
    DATE(submission_timestamp) AS submission_date,
    COALESCE(
      LOGICAL_OR(
        CASE
        WHEN
          event = 'PAGE_TAKEOVER_DATA'
          AND page = 'about:home'
        THEN
          TRUE
        ELSE
          FALSE
        END
      ),
      FALSE
    ) AS activitystream_reported_3rdparty_abouthome,
    COALESCE(
      LOGICAL_OR(
        CASE
        WHEN
          event = 'PAGE_TAKEOVER_DATA'
          AND page = 'about:newtab'
        THEN
          TRUE
        ELSE
          FALSE
        END
      ),
      FALSE
    ) AS activitystream_reported_3rdparty_aboutnewtab,
    COALESCE(
      LOGICAL_OR(CASE WHEN event = 'PAGE_TAKEOVER_DATA' AND page = 'both' THEN TRUE ELSE FALSE END),
      FALSE
    ) AS activitystream_reported_3rdparty_both,
    COALESCE(COUNTIF(event = 'CLICK' AND source = 'TOP_SITES'), 0) AS activitystream_topsite_clicks,
    COALESCE(
      COUNTIF(event = 'CLICK' AND source = 'HIGHLIGHTS'),
      0
    ) AS activitystream_highlight_clicks,
    COALESCE(COUNTIF(event = 'CLICK' AND source = 'CARDGRID'), 0) AS activitystream_pocket_clicks
  FROM
    activity_stream.events
  WHERE
    DATE(submission_timestamp) > start_date
    AND sample_id = 0
    AND normalized_channel = 'release'
  GROUP BY
    client_id,
    submission_date
),
activity_stream_sessions AS (
  SELECT
    client_id,
    DATE(submission_timestamp) AS submission_date,
    COALESCE(MAX(user_prefs & 1 = 0), FALSE) AS activitystream_reported_newtab_search_off,
    COALESCE(MAX(user_prefs & 2 = 0), FALSE) AS activitystream_reported_topsites_off,
    COALESCE(MAX(user_prefs & 4 = 0), FALSE) AS activitystream_reported_pocket_off,
    COALESCE(MAX(user_prefs & 8 = 0), FALSE) AS activitystream_reported_highlights_off,
    COALESCE(MAX(user_prefs & 256 = 0), FALSE) AS activitystream_reported_sponsored_topsites_off,
    COALESCE(COUNTIF(page = 'about:home'), 0) AS activitystream_sessions_abouthome,
    COALESCE(COUNTIF(page = 'about:newtab'), 0) AS activitystream_sessions_newtab,
    COALESCE(COUNTIF(page IN ('about:newtab', 'about:home')), 0) AS activitystream_sessions_both
  FROM
    activity_stream.sessions
  WHERE
    DATE(submission_timestamp) > start_date
    AND sample_id = 0
    AND normalized_channel = 'release'
  GROUP BY
    client_id,
    submission_date
),
addons AS (
  SELECT
    client_id,
    submission_date,
    SUM(
      CASE
      WHEN
        addon_id IN (
          'uBlock0@raymondhill.net',                /* uBlock Origin */
          '{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}', /* Adblock Plus */
          'jid1-NIfFY2CA8fy1tg@jetpack',            /* Adblock */
          '{73a6fe31-595d-460b-a920-fcc0f8843232}', /* NoScript */
          'firefox@ghostery.com',                   /* Ghostery */
          'adblockultimate@adblockultimate.net',    /* AdBlocker Ultimate */
          'jid1-MnnxcxisBPnSXQ@jetpack'             /* Privacy Badger */
        )
      THEN
        1
      ELSE
        0
      END
    ) AS num_addblockers,
    LOGICAL_OR(COALESCE(addon_id = 'notes@mozilla.com', FALSE)) AS has_notes_extension,
    LOGICAL_OR(COALESCE(addon_id = '@contain-facebook', FALSE)) AS has_facebook_container_extension,
    LOGICAL_OR(
      COALESCE(addon_id = '@testpilot-containers', FALSE)
    ) AS has_multiaccount_container_extension,
    LOGICAL_OR(
      COALESCE(addon_id = 'private-relay@firefox.com', FALSE)
    ) AS has_private_relay_extension
  FROM
    telemetry.addons
  WHERE
    submission_date = @submission_date
    AND sample_id = 0
    AND normalized_channel = 'release'
  GROUP BY
    client_id,
    submission_date
)
SELECT
  *
FROM
  user_type
LEFT JOIN
  main
USING
  (client_id, submission_date)
LEFT JOIN
  events
USING
  (client_id, submission_date)
LEFT JOIN
  addons
USING
  (client_id, submission_date)
LEFT JOIN
  activity_stream_events
USING
  (client_id, submission_date)
LEFT JOIN
  activity_stream_sessions
USING
  (client_id, submission_date)
