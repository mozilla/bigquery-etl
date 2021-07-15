-- This script generates the feature usage data for a specific submission_date.
-- Coercions of values is done automatically based on the type and min_version provided
-- in the temporary `version_metadata` table. In particular, all nulls for numerical measures
-- are coalesced to 0 in cases where the client was on an eligible browser version (i.e. a
-- version where the underlying telemetry was implemented); otherwise leave the measure as null
-- for ineligible versions.
-- For adding a new metric:
-- (1) Add metadata to `version_metadata` for the new metric. Metadata is specified as a list of
--   STRUCTs where `metric` specifies the column name for the new feature metric in the result table,
--   `min_version` specifies the minimum eligible browser version and `type` the data type of the
--   computed metric value.
-- (2) Update `feature_usages` and specify the feature metric
-- (3) Update the schema.yaml file and add the new field to the schema
-- (4) Run `./bqetl query schema deploy telemetry_derived.feature_usage_v2 --force` to update the destination
--   table schema.
CREATE TEMP TABLE
  -- This temporary table contains information about metric types and
  -- eligible browser versions.
  version_metadata AS (
    WITH versions AS (
      SELECT
        [
          STRUCT(
            "pdf_viewer_time_to_view_ms_content" AS metric,
            "38" AS min_version,
            "INT64" AS `type`
          ),
          ("pdf_viewer_time_to_view_ms_content_count", "38", "INT64"),
          ("pdf_viewer_doc_size_kb", "38", "INT64"),
          ("pdf_viewer_doc_size_kb_content", "38", "INT64"),
          ("pdf_viewer_doc_size_kb_count", "38", "INT64"),
          ("pdf_viewer_doc_size_kb_content_count", "38", "INT64"),
          ("num_passwords_saved", "38", "INT64"),
          ("os", "42", "STRING"),
          ("normalized_os_version", "42", "STRING"),
          ("app_version", "42", "STRING"),
          ("search_count_all", "43", "INT64"),
          ("is_default_browser", "44", "BOOL"),
          ("sync_signed_in", "44", "BOOL"),
          ("sync_count_desktop_mean", "46", "FLOAT64"),
          ("sync_count_mobile_mean", "46", "FLOAT64"),
          ("scalar_parent_browser_engagement_total_uri_count_sum", "50", "NUMERIC"),
          ("scalar_parent_browser_engagement_tab_open_event_count_sum", "50", "INT64"),
          ("scalar_parent_browser_engagement_window_open_event_count_sum", "50", "INT64"),
          ("scalar_parent_browser_engagement_unique_domains_count_max", "50", "INT64"),
          ("scalar_parent_browser_engagement_unique_domains_count_mean", "50", "FLOAT64"),
          ("scalar_parent_browser_engagement_max_concurrent_tab_count_max", "50", "INT64"),
          ("scalar_parent_browser_engagement_max_concurrent_window_count_max", "50", "INT64"),
          ("attributed", "52", "BOOL"),
          ("uris_from_newtab", "52", "INT64"),
          ("uris_from_searchbar", "52", "INT64"),
          ("uris_from_urlbar", "52", "INT64"),
          ("active_hours_sum", "56", "FLOAT64"),
          ("newtab_click", "60", "INT64"),
          ("bookmark_added_from_newtab", "60", "INT64"),
          ("saved_to_pocket_from_newtab", "60", "INT64"),
          ("newtab_prefs_opened", "60", "INT64"),
          ("pbm_used", "64", "BOOL"),
          ("ad_clicks_count_all", "65", "INT64"),
          ("search_with_ads_count_all", "65", "INT64"),
          ("installed_extension", "67", "INT64"),
          ("installed_theme", "67", "INT64"),
          ("installed_l10n", "67", "INT64"),
          ("normandy_enrolled", "67", "INT64"),
          ("pwmgr_opened", "68", "INT64"),
          ("pip_window_open_duration", "69", "INT64"),
          ("pip_window_open_duration_count", "69", "INT64"),
          ("generated_password", "69", "INT64"),
          ("video_play_time_ms", "70", "INT64"),
          ("video_encrypted_play_time_ms_count", "70", "INT64"),
          ("pip_count", "70", "INT64"),
          ("viewed_protection_report_count", "70", "INT64"),
          ("etp_toggle_off", "70", "INT64"),
          ("etp_toggle_on", "70", "INT64"),
          ("protections_popup", "70", "INT64"),
          ("pwmgr_interacted_breach", "71", "INT64"),
          ("is_headless", "72", "BOOL"),
          ("video_play_time_ms_count", "72", "INT64"),
          ("fxa_connect", "72", "INT64"),
          ("nav_history_urlbar", "78", "INT64"),
          ("nav_autocomplete_urlbar", "78", "INT64"),
          ("nav_visiturl_urlbar", "78", "INT64"),
          ("nav_searchsuggestion_urlbar", "78", "INT64"),
          ("nav_topsite_urlbar", "78", "INT64"),
          ("used_stored_pw", "78", "INT64"),
          ("password_filled", "78", "INT64"),
          ("pwmgr_copy_or_show_info", "78", "INT64"),
          ("unique_preferences_accessed_count", "79", "INT64"),
          ("preferences_accessed_total", "79", "INT64"),
          ("unique_bookmarks_bar_accessed_count", "79", "INT64"),
          ("bookmarks_bar_accessed_total", "79", "INT64"),
          ("unique_keyboard_shortcut_count", "79", "INT64"),
          ("keyboard_shortcut_total", "79", "INT64"),
          ("downloads", "79", "INT64"),
          ("pdf_downloads", "79", "INT64"),
          ("image_downloads", "79", "INT64"),
          ("media_downloads", "79", "INT64"),
          ("msoffice_downloads", "79", "INT64"),
          ("password_saved", "80", "INT64"),
          ("ccards_saved", "81", "BOOL"),
          ("unique_sidebars_accessed_count", "81", "INT64"),
          ("sidebars_accessed_total", "81", "INT64"),
          ("ccard_filled", "81", "INT64"),
          ("ccard_saved", "81", "INT64"),
          ("video_encrypted_play_time_ms", "82", "INT64"),
          ("unique_history_urlbar_indices_picked_count", "84", "INT64"),
          ("history_urlbar_picked_total", "84", "INT64"),
          ("unique_remotetab_indices_picked_count", "84", "INT64"),
          ("remotetab_picked_total", "84", "INT64"),
          ("search_count_abouthome", "86", "INT64"),
          ("search_count_contextmenu", "86", "INT64"),
          ("search_count_newtab", "86", "INT64"),
          ("search_count_organic", "86", "INT64"),
          ("search_count_searchbar", "86", "INT64"),
          ("search_count_system", "86", "INT64"),
          ("search_count_tagged_follow_on", "86", "INT64"),
          ("search_count_tagged_sap", "86", "INT64"),
          ("search_count_urlbar", "86", "INT64"),
          ("scalar_parent_os_environment_is_taskbar_pinned", "88", "BOOL"),
          ("scalar_parent_os_environment_launched_via_desktop", "88", "BOOL"),
          ("scalar_parent_os_environment_launched_via_other", "88", "BOOL"),
          ("scalar_parent_os_environment_launched_via_taskbar", "88", "BOOL"),
          ("scalar_parent_os_environment_launched_via_start_menu", "88", "BOOL"),
          ("scalar_parent_os_environment_launched_via_other_shortcut", "88", "BOOL")
        ] v
    )
    SELECT
      v.metric,
      v.min_version,
      v.type
    FROM
      versions,
      UNNEST(versions.v) AS v
  );

-- This table defines the specific features we are interested in
CREATE TEMP TABLE
  feature_usages AS (
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
          CAST(
            JSON_EXTRACT_SCALAR(
              payload.processes.content.histograms.video_play_time_ms,
              '$.sum'
            ) AS int64
          )
        ) AS video_play_time_ms,
        SUM(
          CAST(
            JSON_EXTRACT_SCALAR(
              payload.processes.content.histograms.video_encrypted_play_time_ms,
              '$.sum'
            ) AS int64
          )
        ) AS video_encrypted_play_time_ms,
        SUM(
          CAST(
            JSON_EXTRACT_SCALAR(
              payload.processes.content.histograms.pdf_viewer_time_to_view_ms,
              '$.sum'
            ) AS int64
          )
        ) AS pdf_viewer_time_to_view_ms_content,
        SUM(
          CAST(
            JSON_EXTRACT_SCALAR(
              payload.histograms.fx_picture_in_picture_window_open_duration,
              '$.sum'
            ) AS int64
          )
        ) AS pip_window_open_duration,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(
                mozfun.hist.extract(payload.processes.content.histograms.video_play_time_ms).values
              )
          )
        ) AS video_play_time_ms_count,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(
                mozfun.hist.extract(
                  payload.processes.content.histograms.video_encrypted_play_time_ms
                ).values
              )
          )
        ) AS video_encrypted_play_time_ms_count,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(
                mozfun.hist.extract(
                  payload.processes.content.histograms.pdf_viewer_time_to_view_ms
                ).values
              )
          )
        ) AS pdf_viewer_time_to_view_ms_content_count,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(
                mozfun.hist.extract(
                  payload.histograms.fx_picture_in_picture_window_open_duration
                ).values
              )
          )
        ) AS pip_window_open_duration_count,
        SUM(
          CAST(
            JSON_EXTRACT_SCALAR(payload.histograms.pdf_viewer_document_size_kb, '$.sum') AS int64
          )
        ) AS pdf_viewer_doc_size_kb,
        SUM(
          CAST(
            JSON_EXTRACT_SCALAR(
              payload.processes.content.histograms.pdf_viewer_document_size_kb,
              '$.sum'
            ) AS int64
          )
        ) AS pdf_viewer_doc_size_kb_content,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(mozfun.hist.extract(payload.histograms.pdf_viewer_document_size_kb).values)
          )
        ) AS pdf_viewer_doc_size_kb_count,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(
                mozfun.hist.extract(
                  payload.processes.content.histograms.pdf_viewer_document_size_kb
                ).values
              )
          )
        ) AS pdf_viewer_doc_size_kb_content_count,
        COUNTIF(environment.services.account_enabled) > 0 AS sync_signed_in,
        COUNTIF(
          payload.processes.parent.scalars.formautofill_credit_cards_autofill_profiles_count IS NOT NULL
        ) > 0 AS ccards_saved,
        COUNTIF(
          payload.processes.parent.scalars.dom_parentprocess_private_window_used
        ) > 0 AS pbm_used,
        SUM(
          ARRAY_LENGTH(payload.processes.parent.keyed_scalars.sidebar_opened)
        ) AS unique_sidebars_accessed_count,
        SUM(
          (SELECT SUM(value) FROM UNNEST(payload.processes.parent.keyed_scalars.sidebar_opened))
        ) AS sidebars_accessed_total,
        SUM(
          ARRAY_LENGTH(payload.processes.parent.keyed_scalars.urlbar_picked_history)
        ) AS unique_history_urlbar_indices_picked_count,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(payload.processes.parent.keyed_scalars.urlbar_picked_history)
          )
        ) AS history_urlbar_picked_total,
        SUM(
          ARRAY_LENGTH(payload.processes.parent.keyed_scalars.urlbar_picked_remotetab)
        ) AS unique_remotetab_indices_picked_count,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(payload.processes.parent.keyed_scalars.urlbar_picked_remotetab)
          )
        ) AS remotetab_picked_total,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(
                payload.processes.parent.keyed_scalars.browser_engagement_navigation_about_newtab
              )
          )
        ) AS uris_from_newtab,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(payload.processes.parent.keyed_scalars.browser_engagement_navigation_searchbar)
          )
        ) AS uris_from_searchbar,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(payload.processes.parent.keyed_scalars.browser_engagement_navigation_urlbar)
          )
        ) AS uris_from_urlbar,
        SUM(
          mozfun.map.get_key(
            mozfun.hist.extract(payload.histograms.fx_urlbar_selected_result_type_2).values,
            2
          )
        ) AS nav_history_urlbar,
        SUM(
          mozfun.map.get_key(
            mozfun.hist.extract(payload.histograms.fx_urlbar_selected_result_type_2).values,
            0
          )
        ) AS nav_autocomplete_urlbar,
        SUM(
          mozfun.map.get_key(
            mozfun.hist.extract(payload.histograms.fx_urlbar_selected_result_type_2).values,
            8
          )
        ) AS nav_visiturl_urlbar,
        SUM(
          mozfun.map.get_key(
            mozfun.hist.extract(payload.histograms.fx_urlbar_selected_result_type_2).values,
            5
          )
        ) AS nav_searchsuggestion_urlbar,
        SUM(
          mozfun.map.get_key(
            mozfun.hist.extract(payload.histograms.fx_urlbar_selected_result_type_2).values,
            13
          )
        ) AS nav_topsite_urlbar,
        MAX(
          CAST(JSON_EXTRACT_SCALAR(payload.histograms.pwmgr_num_saved_passwords, '$.sum') AS int64)
        ) AS num_passwords_saved,
        SUM(
          ARRAY_LENGTH(
            payload.processes.parent.keyed_scalars.browser_ui_interaction_preferences_pane_general
          )
        ) AS unique_preferences_accessed_count,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(
                payload.processes.parent.keyed_scalars.browser_ui_interaction_preferences_pane_general
              )
          )
        ) AS preferences_accessed_total,
        SUM(
          ARRAY_LENGTH(payload.processes.parent.keyed_scalars.browser_ui_interaction_bookmarks_bar)
        ) AS unique_bookmarks_bar_accessed_count,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(payload.processes.parent.keyed_scalars.browser_ui_interaction_bookmarks_bar)
          )
        ) AS bookmarks_bar_accessed_total,
        SUM(
          ARRAY_LENGTH(payload.processes.parent.keyed_scalars.browser_ui_interaction_keyboard)
        ) AS unique_keyboard_shortcut_count,
        SUM(
          (
            SELECT
              SUM(value)
            FROM
              UNNEST(payload.processes.parent.keyed_scalars.browser_ui_interaction_keyboard)
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
        COUNTIF(event_category = 'pictureinpicture' AND event_method = 'create') AS pip_count,
        COUNTIF(
          event_category = 'security.ui.protections'
          AND event_object = 'protection_report'
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
        COUNTIF(
          event_category = 'creditcard'
          AND event_object = 'cc_form'
          AND event_method = 'filled'
        ) AS ccard_filled,
        COUNTIF(
          event_category = 'creditcard'
          AND event_object = 'capture_doorhanger'
          AND event_method = 'save'
        ) AS ccard_saved,
        COUNTIF(
          event_method = 'install'
          AND event_category = 'addonsManager'
          AND event_object = 'extension'
        ) AS installed_extension,
        COUNTIF(
          event_method = 'install'
          AND event_category = 'addonsManager'
          AND event_object = 'theme'
        ) AS installed_theme,
        COUNTIF(
          event_method = 'install'
          AND event_category = 'addonsManager'
          AND event_object IN ('dictionary', 'locale')
        ) AS installed_l10n,
        COUNTIF(event_method = 'saved_login_used') AS used_stored_pw,
        COUNTIF(
          event_category = 'pwmgr'
          AND event_object IN ('form_login', 'form_password', 'auth_login', 'prompt_login')
        ) AS password_filled,
        COUNTIF(
          event_category = 'pwmgr'
          AND event_method = 'doorhanger_submitted'
          AND event_object = 'save'
        ) AS password_saved,
        COUNTIF(event_category = 'pwmgr' AND event_method = 'open_management') AS pwmgr_opened,
        COUNTIF(
          event_category = 'pwmgr'
          AND event_method IN ('copy', 'show')
        ) AS pwmgr_copy_or_show_info,
        COUNTIF(
          event_category = 'pwmgr'
          AND event_method IN ('dismiss_breach_alert', 'learn_more_breach')
        ) AS pwmgr_interacted_breach,
        COUNTIF(
          event_object = 'generatedpassword'
          AND event_method = 'autocomplete_field'
        ) AS generated_password,
        COUNTIF(event_category = 'fxa' AND event_method = 'connect') AS fxa_connect,
        COUNTIF(
          event_category = 'normandy'
          AND event_object IN (
            "preference_study",
            "addon_study",
            "preference_rollout",
            "addon_rollout"
          )
        ) AS normandy_enrolled,
        COUNTIF(event_category = 'downloads') AS downloads,
        COUNTIF(event_category = 'downloads' AND event_string_value = 'pdf') AS pdf_downloads,
        COUNTIF(
          event_category = 'downloads'
          AND event_string_value IN ('jpg', 'jpeg', 'png', 'gif')
        ) AS image_downloads,
        COUNTIF(
          event_category = 'downloads'
          AND event_string_value IN ('mp4', 'mp3', 'wav', 'mov')
        ) AS media_downloads,
        COUNTIF(
          event_category = 'downloads'
          AND event_string_value IN ('xlsx', 'docx', 'pptx', 'xls', 'ppt', 'doc')
        ) AS msoffice_downloads,
        COUNTIF(event_category = 'activity_stream' AND event_object IN ('CLICK')) AS newtab_click,
        COUNTIF(
          event_category = 'activity_stream'
          AND event_object IN ('BOOKMARK_ADD')
        ) AS bookmark_added_from_newtab,
        COUNTIF(
          event_category = 'activity_stream'
          AND event_object IN ('SAVE_TO_POCKET')
        ) AS saved_to_pocket_from_newtab,
        COUNTIF(
          event_category = 'activity_stream'
          AND event_object IN ('OPEN_NEWTAB_PREFS')
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
        ) AS activitystream_reported_3rdparty_abouthome,
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
        ) AS activitystream_reported_3rdparty_aboutnewtab,
        LOGICAL_OR(
          CASE
          WHEN
            event = 'PAGE_TAKEOVER_DATA'
            AND page = 'both'
          THEN
            TRUE
          ELSE
            FALSE
          END
        ) AS activitystream_reported_3rdparty_both,
        COUNTIF(event = 'CLICK' AND source = 'TOP_SITES') AS activitystream_topsite_clicks,
        COUNTIF(event = 'CLICK' AND source = 'HIGHLIGHTS') AS activitystream_highlight_clicks,
        COUNTIF(event = 'CLICK' AND source = 'CARDGRID') AS activitystream_pocket_clicks
      FROM
        activity_stream.events
      WHERE
        DATE(submission_timestamp) = @submission_date
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
        MAX(user_prefs & 1 = 0) AS activitystream_reported_newtab_search_off,
        MAX(user_prefs & 2 = 0) AS activitystream_reported_topsites_off,
        MAX(user_prefs & 4 = 0) AS activitystream_reported_pocket_off,
        MAX(user_prefs & 8 = 0) AS activitystream_reported_highlights_off,
        MAX(user_prefs & 256 = 0) AS activitystream_reported_sponsored_topsites_off,
        COUNTIF(page = 'about:home') AS activitystream_sessions_abouthome,
        COUNTIF(page = 'about:newtab') AS activitystream_sessions_newtab,
        COUNTIF(page IN ('about:newtab', 'about:home')) AS activitystream_sessions_both
      FROM
        activity_stream.sessions
      WHERE
        DATE(submission_timestamp) = @submission_date
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
        LOGICAL_OR(
          COALESCE(addon_id = '@contain-facebook', FALSE)
        ) AS has_facebook_container_extension,
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
  );

-- To ensure backfills replace existing data in the feature usage table,
-- existing data needs to be deleted explicitly since scripts do not
-- allow to replace table partitions or writing of results to tables.
DELETE FROM
  telemetry_derived.feature_usage_v2
WHERE
  submission_date = @submission_date;

-- The following part applies COALESCE to feature metrics and checks for
-- eligible versions.
-- The query that is generated and executed here looks like the following:
--   INSERT INTO telemetry_derived.feature_usage_v2
--   SELECT
--     client_id,
--     ...
--     IF("78" > f.app_version, NULL, COALESCE(password_filled, CAST(0 AS INT64))) AS password_filled,
--     ...
--     IF("52" > f.app_version, NULL, COALESCE(attributed, CAST(0 AS BOOL))) AS attributed,
--     ...
--   FROM feature_usages f
EXECUTE IMMEDIATE(
  SELECT
    -- Scripts do not return result data that could be written to a partition with
    -- our existing machinery, so we have to INSERT the data into the destintion table.
    'INSERT INTO telemetry_derived.feature_usage_v2 ' || 'SELECT ' || STRING_AGG(
      IF(
        `type` IS NULL, -- if no metadata is provided in `version_metadata` take the value as is
        column,
        -- otherwise use 'IF("<version>" > f.app_version, NULL, COALESCE(<metric>, CAST(0 AS <type>))) AS <metric>,'
        -- in query
        'IF("' || min_version || '" > f.app_version, NULL, COALESCE(' || column || ', CAST(0 AS ' || `type` || '))) AS ' || column
      )
    ) || ' FROM feature_usages f'
  FROM
    (
      -- This extracts all the column names (=metrics) from the temporary feature usage table.
      SELECT
        REGEXP_EXTRACT_ALL(to_json_string(f), r'"([^"]*)":') AS columns
      FROM
        feature_usages f
      LIMIT
        1
    ),
    UNNEST(columns) column
  LEFT JOIN
    version_metadata
  ON
    metric = column
);
