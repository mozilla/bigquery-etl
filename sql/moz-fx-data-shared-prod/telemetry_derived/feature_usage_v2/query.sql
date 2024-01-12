-- Generated via ./bqetl generate feature_usage
-- Don't edit this file directly.
-- To add new metrics, please update sql_generators/feature_usage/templating.yaml
-- and run `./bqetl generate feature_usage`.
WITH user_type AS (
  SELECT
    client_id AS client_id,
    submission_date AS submission_date,
    activity_segments_v1 AS activity_segments_v1,
    is_allweek_regular_v1 AS is_allweek_regular_v1,
    is_weekday_regular_v1 AS is_weekday_regular_v1,
    is_core_active_v1 AS is_core_active_v1,
    days_since_first_seen AS days_since_first_seen,
    days_since_seen AS days_since_seen,
    new_profile_7_day_activated_v1 AS new_profile_7_day_activated_v1,
    new_profile_14_day_activated_v1 AS new_profile_14_day_activated_v1,
    new_profile_21_day_activated_v1 AS new_profile_21_day_activated_v1,
    first_seen_date AS first_seen_date,
    days_since_created_profile AS days_since_created_profile,
    profile_creation_date AS profile_creation_date,
    country AS country,
    scalar_parent_os_environment_is_taskbar_pinned AS scalar_parent_os_environment_is_taskbar_pinned,
    scalar_parent_os_environment_launched_via_desktop AS scalar_parent_os_environment_launched_via_desktop,
    scalar_parent_os_environment_launched_via_other AS scalar_parent_os_environment_launched_via_other,
    scalar_parent_os_environment_launched_via_taskbar AS scalar_parent_os_environment_launched_via_taskbar,
    scalar_parent_os_environment_launched_via_start_menu AS scalar_parent_os_environment_launched_via_start_menu,
    scalar_parent_os_environment_launched_via_other_shortcut AS scalar_parent_os_environment_launched_via_other_shortcut,
    os AS os,
    normalized_os_version AS normalized_os_version,
    app_version AS app_version,
    (attribution.campaign IS NOT NULL)
    OR (attribution.source IS NOT NULL) AS attributed,
    is_default_browser AS is_default_browser,
    sync_count_desktop_mean AS sync_count_desktop_mean,
    sync_count_mobile_mean AS sync_count_mobile_mean,
    active_hours_sum AS active_hours_sum,
    subsession_hours_sum AS scalar_parent_browser_engagement_total_uri_count_sum,
    ad_clicks_count_all AS ad_clicks_count_all,
    scalar_parent_browser_engagement_tab_open_event_count_sum AS scalar_parent_browser_engagement_tab_open_event_count_sum,
    scalar_parent_browser_engagement_window_open_event_count_sum AS scalar_parent_browser_engagement_window_open_event_count_sum,
    scalar_parent_browser_engagement_unique_domains_count_max AS scalar_parent_browser_engagement_unique_domains_count_max,
    scalar_parent_browser_engagement_unique_domains_count_mean AS scalar_parent_browser_engagement_unique_domains_count_mean,
    scalar_parent_browser_engagement_max_concurrent_tab_count_max AS scalar_parent_browser_engagement_max_concurrent_tab_count_max,
    scalar_parent_browser_engagement_max_concurrent_window_count_max AS scalar_parent_browser_engagement_max_concurrent_window_count_max,
    search_count_abouthome AS search_count_abouthome,
    search_count_all AS search_count_all,
    search_count_contextmenu AS search_count_contextmenu,
    search_count_newtab AS search_count_newtab,
    search_count_organic AS search_count_organic,
    search_count_searchbar AS search_count_searchbar,
    search_count_system AS search_count_system,
    search_count_tagged_follow_on AS search_count_tagged_follow_on,
    search_count_tagged_sap AS search_count_tagged_sap,
    search_count_urlbar AS search_count_urlbar,
    search_count_urlbar_handoff AS search_count_urlbar_handoff,
    search_with_ads_count_all AS search_with_ads_count_all,
    SAFE_CAST(user_pref_browser_newtabpage_enabled AS BOOL) AS newtabpage_disabled,
    (
      SELECT
        SUM(IF(SPLIT(key, '_')[SAFE_OFFSET(0)] = 'newtab', value, 0))
      FROM
        UNNEST(contextual_services_topsites_impression_sum)
    ) AS num_topsites_new_tab_impressions_sponsored,
    (
      SELECT
        SUM(IF(SPLIT(key, '_')[SAFE_OFFSET(0)] = 'newtab', value, 0))
      FROM
        UNNEST(contextual_services_topsites_click_sum)
    ) AS num_new_tab_topsites_clicks_sponsored,
  FROM
    telemetry.clients_last_seen
  WHERE
    submission_date = @submission_date
    AND sample_id = 0
    AND normalized_channel = 'release'
    AND days_since_seen = 0
),
main AS (
  SELECT
    client_id AS client_id,
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
      CAST(JSON_EXTRACT_SCALAR(payload.histograms.pdf_viewer_document_size_kb, '$.sum') AS int64)
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
    COUNTIF(payload.processes.parent.scalars.dom_parentprocess_private_window_used) > 0 AS pbm_used,
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
      (SELECT SUM(value) FROM UNNEST(payload.processes.parent.keyed_scalars.urlbar_picked_history))
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
          UNNEST(payload.processes.parent.keyed_scalars.browser_engagement_navigation_about_newtab)
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
    telemetry.main_remainder_1pct
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
    client_id AS client_id,
    submission_date AS submission_date,
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
      AND event_object IN ("preference_study", "addon_study", "preference_rollout", "addon_rollout")
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
    ) AS newtab_prefs_opened,
  FROM
    telemetry.events
  WHERE
    submission_date = @submission_date
    AND sample_id = 0
    AND normalized_channel = 'release'
  GROUP BY
    submission_date,
    client_id
),
activity_stream_events AS (
  SELECT
    client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    LOGICAL_OR(
      CASE
        WHEN event = 'PAGE_TAKEOVER_DATA'
          AND page = 'about:home'
          THEN TRUE
        ELSE FALSE
      END
    ) AS activitystream_reported_3rdparty_abouthome,
    LOGICAL_OR(
      CASE
        WHEN event = 'PAGE_TAKEOVER_DATA'
          AND page = 'about:newtab'
          THEN TRUE
        ELSE FALSE
      END
    ) AS activitystream_reported_3rdparty_aboutnewtab,
    LOGICAL_OR(
      CASE
        WHEN event = 'PAGE_TAKEOVER_DATA'
          AND page = 'both'
          THEN TRUE
        ELSE FALSE
      END
    ) AS activitystream_reported_3rdparty_both,
    COUNTIF(event = 'CLICK' AND source = 'TOP_SITES') AS activitystream_topsite_clicks,
    COUNTIF(event = 'CLICK' AND source = 'HIGHLIGHTS') AS activitystream_highlight_clicks,
    COUNTIF(event = 'CLICK' AND source = 'CARDGRID') AS activitystream_pocket_clicks,
    COUNTIF(
      event = 'CLICK'
      AND source = 'CARDGRID'
      AND JSON_EXTRACT_SCALAR(value, '$.card_type') = 'spoc'
    ) AS activitystream_sponsored_pocket_clicks,
    COUNTIF(
      event = 'CLICK'
      AND source = 'TOP_SITES'
      AND JSON_EXTRACT_SCALAR(value, '$.card_type') = 'spoc'
    ) AS activitystream_sponsored_topsite_clicks,
    COUNTIF(
      event = 'CLICK'
      AND source = 'CARDGRID'
      AND JSON_EXTRACT_SCALAR(value, '$.card_type') = 'organic'
    ) AS activitystream_organic_pocket_clicks,
    COUNTIF(
      event = 'CLICK'
      AND source = 'TOP_SITES'
      AND JSON_EXTRACT_SCALAR(value, '$.card_type') = 'organic'
    ) AS activitystream_organic_topsite_clicks,
  FROM
    activity_stream.events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id = 0
    AND normalized_channel = 'release'
  GROUP BY
    submission_date,
    client_id
),
activity_stream_sessions AS (
  SELECT
    client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    MAX(user_prefs & 1 = 0) AS activitystream_reported_newtab_search_off,
    MAX(user_prefs & 2 = 0) AS activitystream_reported_topsites_off,
    MAX(user_prefs & 4 = 0) AS activitystream_reported_pocket_off,
    MAX(user_prefs & 8 = 0) AS activitystream_reported_highlights_off,
    MAX(user_prefs & 32 = 0) AS activitystream_reported_sponsored_topstories_off,
    MAX(user_prefs & 256 = 0) AS activitystream_reported_sponsored_topsites_off,
    COUNTIF(page = 'about:home') AS activitystream_sessions_abouthome,
    COUNTIF(page = 'about:newtab') AS activitystream_sessions_newtab,
    COUNTIF(page IN ('about:newtab', 'about:home')) AS activitystream_sessions_both,
  FROM
    activity_stream.sessions
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id = 0
    AND normalized_channel = 'release'
  GROUP BY
    submission_date,
    client_id
),
addons AS (
  SELECT
    client_id AS client_id,
    submission_date AS submission_date,
    SUM(
      CASE
        WHEN addon_id IN (
            'uBlock0@raymondhill.net',                /* uBlock Origin */
            '{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}', /* Adblock Plus */
            'jid1-NIfFY2CA8fy1tg@jetpack',            /* Adblock */
            '{73a6fe31-595d-460b-a920-fcc0f8843232}', /* NoScript */
            'firefox@ghostery.com',                   /* Ghostery */
            'adblockultimate@adblockultimate.net', /* AdBlocker Ultimate */
            'jid1-MnnxcxisBPnSXQ@jetpack'             /* Privacy Badger */
          )
          THEN 1
        ELSE 0
      END
    ) AS num_addblockers,
    LOGICAL_OR(COALESCE(addon_id = 'notes@mozilla.com', FALSE)) AS has_notes_extension,
    LOGICAL_OR(COALESCE(addon_id = '@contain-facebook', FALSE)) AS has_facebook_container_extension,
    LOGICAL_OR(
      COALESCE(addon_id = '@testpilot-containers', FALSE)
    ) AS has_multiaccount_container_extension,
    LOGICAL_OR(
      COALESCE(addon_id = 'private-relay@firefox.com', FALSE)
    ) AS has_private_relay_extension,
  FROM
    telemetry.addons
  WHERE
    submission_date = @submission_date
    AND sample_id = 0
    AND normalized_channel = 'release'
  GROUP BY
    submission_date,
    client_id
),
all_features AS (
  SELECT
    *
  FROM
    user_type
  LEFT JOIN
    main
    USING (client_id, submission_date)
  LEFT JOIN
    events
    USING (client_id, submission_date)
  LEFT JOIN
    activity_stream_events
    USING (client_id, submission_date)
  LEFT JOIN
    activity_stream_sessions
    USING (client_id, submission_date)
  LEFT JOIN
    addons
    USING (client_id, submission_date)
)
SELECT
  client_id,
  submission_date,
  activity_segments_v1,
  is_allweek_regular_v1,
  is_weekday_regular_v1,
  is_core_active_v1,
  days_since_first_seen,
  days_since_seen,
  new_profile_7_day_activated_v1,
  new_profile_14_day_activated_v1,
  new_profile_21_day_activated_v1,
  first_seen_date,
  days_since_created_profile,
  profile_creation_date,
  country,
  IF(
    88 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_os_environment_is_taskbar_pinned, CAST(0 AS BOOL)),
    NULL
  ) AS scalar_parent_os_environment_is_taskbar_pinned,
  IF(
    88 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_os_environment_launched_via_desktop, CAST(0 AS BOOL)),
    NULL
  ) AS scalar_parent_os_environment_launched_via_desktop,
  IF(
    88 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_os_environment_launched_via_other, CAST(0 AS BOOL)),
    NULL
  ) AS scalar_parent_os_environment_launched_via_other,
  IF(
    88 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_os_environment_launched_via_taskbar, CAST(0 AS BOOL)),
    NULL
  ) AS scalar_parent_os_environment_launched_via_taskbar,
  IF(
    88 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_os_environment_launched_via_start_menu, CAST(0 AS BOOL)),
    NULL
  ) AS scalar_parent_os_environment_launched_via_start_menu,
  IF(
    88 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_os_environment_launched_via_other_shortcut, CAST(0 AS BOOL)),
    NULL
  ) AS scalar_parent_os_environment_launched_via_other_shortcut,
  IF(
    42 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(os, CAST(0 AS STRING)),
    NULL
  ) AS os,
  IF(
    42 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(normalized_os_version, CAST(0 AS STRING)),
    NULL
  ) AS normalized_os_version,
  IF(
    42 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(app_version, CAST(0 AS STRING)),
    NULL
  ) AS app_version,
  attributed,
  IF(
    44 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(is_default_browser, CAST(0 AS BOOL)),
    NULL
  ) AS is_default_browser,
  IF(
    46 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(sync_count_desktop_mean, CAST(0 AS FLOAT64)),
    NULL
  ) AS sync_count_desktop_mean,
  IF(
    46 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(sync_count_mobile_mean, CAST(0 AS FLOAT64)),
    NULL
  ) AS sync_count_mobile_mean,
  IF(
    56 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(active_hours_sum, CAST(0 AS FLOAT64)),
    NULL
  ) AS active_hours_sum,
  IF(
    50 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_browser_engagement_total_uri_count_sum, CAST(0 AS NUMERIC)),
    NULL
  ) AS scalar_parent_browser_engagement_total_uri_count_sum,
  IF(
    65 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(ad_clicks_count_all, CAST(0 AS INT64)),
    NULL
  ) AS ad_clicks_count_all,
  IF(
    50 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_browser_engagement_tab_open_event_count_sum, CAST(0 AS INT64)),
    NULL
  ) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
  IF(
    50 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_browser_engagement_window_open_event_count_sum, CAST(0 AS INT64)),
    NULL
  ) AS scalar_parent_browser_engagement_window_open_event_count_sum,
  IF(
    50 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_browser_engagement_unique_domains_count_max, CAST(0 AS INT64)),
    NULL
  ) AS scalar_parent_browser_engagement_unique_domains_count_max,
  IF(
    50 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_browser_engagement_unique_domains_count_mean, CAST(0 AS INT64)),
    NULL
  ) AS scalar_parent_browser_engagement_unique_domains_count_mean,
  IF(
    50 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_browser_engagement_max_concurrent_tab_count_max, CAST(0 AS INT64)),
    NULL
  ) AS scalar_parent_browser_engagement_max_concurrent_tab_count_max,
  IF(
    50 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(scalar_parent_browser_engagement_max_concurrent_window_count_max, CAST(0 AS INT64)),
    NULL
  ) AS scalar_parent_browser_engagement_max_concurrent_window_count_max,
  IF(
    86 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_abouthome, CAST(0 AS INT64)),
    NULL
  ) AS search_count_abouthome,
  IF(
    43 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_all, CAST(0 AS INT64)),
    NULL
  ) AS search_count_all,
  IF(
    86 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_contextmenu, CAST(0 AS INT64)),
    NULL
  ) AS search_count_contextmenu,
  IF(
    86 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_newtab, CAST(0 AS INT64)),
    NULL
  ) AS search_count_newtab,
  IF(
    86 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_organic, CAST(0 AS INT64)),
    NULL
  ) AS search_count_organic,
  IF(
    86 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_searchbar, CAST(0 AS INT64)),
    NULL
  ) AS search_count_searchbar,
  IF(
    86 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_system, CAST(0 AS INT64)),
    NULL
  ) AS search_count_system,
  IF(
    86 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_tagged_follow_on, CAST(0 AS INT64)),
    NULL
  ) AS search_count_tagged_follow_on,
  IF(
    86 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_tagged_sap, CAST(0 AS INT64)),
    NULL
  ) AS search_count_tagged_sap,
  IF(
    86 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_urlbar, CAST(0 AS INT64)),
    NULL
  ) AS search_count_urlbar,
  IF(
    94 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_count_urlbar_handoff, CAST(0 AS INT64)),
    NULL
  ) AS search_count_urlbar_handoff,
  IF(
    64 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(search_with_ads_count_all, CAST(0 AS INT64)),
    NULL
  ) AS search_with_ads_count_all,
  newtabpage_disabled,
  IF(
    87 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(num_topsites_new_tab_impressions_sponsored, CAST(0 AS INT64)),
    NULL
  ) AS num_topsites_new_tab_impressions_sponsored,
  IF(
    87 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(num_new_tab_topsites_clicks_sponsored, CAST(0 AS INT64)),
    NULL
  ) AS num_new_tab_topsites_clicks_sponsored,
  subsample_id,
  IF(
    72 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(is_headless, CAST(0 AS BOOL)),
    NULL
  ) AS is_headless,
  IF(
    70 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(video_play_time_ms, CAST(0 AS INT64)),
    NULL
  ) AS video_play_time_ms,
  IF(
    82 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(video_encrypted_play_time_ms, CAST(0 AS INT64)),
    NULL
  ) AS video_encrypted_play_time_ms,
  IF(
    38 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pdf_viewer_time_to_view_ms_content, CAST(0 AS INT64)),
    NULL
  ) AS pdf_viewer_time_to_view_ms_content,
  IF(
    69 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pip_window_open_duration, CAST(0 AS INT64)),
    NULL
  ) AS pip_window_open_duration,
  IF(
    72 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(video_play_time_ms_count, CAST(0 AS INT64)),
    NULL
  ) AS video_play_time_ms_count,
  IF(
    70 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(video_encrypted_play_time_ms_count, CAST(0 AS INT64)),
    NULL
  ) AS video_encrypted_play_time_ms_count,
  IF(
    38 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pdf_viewer_time_to_view_ms_content_count, CAST(0 AS INT64)),
    NULL
  ) AS pdf_viewer_time_to_view_ms_content_count,
  IF(
    69 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pip_window_open_duration_count, CAST(0 AS INT64)),
    NULL
  ) AS pip_window_open_duration_count,
  IF(
    38 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pdf_viewer_doc_size_kb, CAST(0 AS INT64)),
    NULL
  ) AS pdf_viewer_doc_size_kb,
  IF(
    38 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pdf_viewer_doc_size_kb_content, CAST(0 AS INT64)),
    NULL
  ) AS pdf_viewer_doc_size_kb_content,
  IF(
    38 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pdf_viewer_doc_size_kb_count, CAST(0 AS INT64)),
    NULL
  ) AS pdf_viewer_doc_size_kb_count,
  IF(
    38 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pdf_viewer_doc_size_kb_content_count, CAST(0 AS INT64)),
    NULL
  ) AS pdf_viewer_doc_size_kb_content_count,
  IF(
    44 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(sync_signed_in, CAST(0 AS BOOL)),
    NULL
  ) AS sync_signed_in,
  IF(
    81 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(ccards_saved, CAST(0 AS BOOL)),
    NULL
  ) AS ccards_saved,
  IF(
    64 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pbm_used, CAST(0 AS BOOL)),
    NULL
  ) AS pbm_used,
  IF(
    81 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(unique_sidebars_accessed_count, CAST(0 AS INT64)),
    NULL
  ) AS unique_sidebars_accessed_count,
  IF(
    81 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(sidebars_accessed_total, CAST(0 AS INT64)),
    NULL
  ) AS sidebars_accessed_total,
  IF(
    84 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(unique_history_urlbar_indices_picked_count, CAST(0 AS INT64)),
    NULL
  ) AS unique_history_urlbar_indices_picked_count,
  IF(
    84 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(history_urlbar_picked_total, CAST(0 AS INT64)),
    NULL
  ) AS history_urlbar_picked_total,
  IF(
    84 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(unique_remotetab_indices_picked_count, CAST(0 AS INT64)),
    NULL
  ) AS unique_remotetab_indices_picked_count,
  IF(
    84 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(remotetab_picked_total, CAST(0 AS INT64)),
    NULL
  ) AS remotetab_picked_total,
  IF(
    52 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(uris_from_newtab, CAST(0 AS INT64)),
    NULL
  ) AS uris_from_newtab,
  IF(
    52 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(uris_from_searchbar, CAST(0 AS INT64)),
    NULL
  ) AS uris_from_searchbar,
  IF(
    52 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(uris_from_urlbar, CAST(0 AS INT64)),
    NULL
  ) AS uris_from_urlbar,
  IF(
    78 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(nav_history_urlbar, CAST(0 AS INT64)),
    NULL
  ) AS nav_history_urlbar,
  IF(
    78 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(nav_autocomplete_urlbar, CAST(0 AS INT64)),
    NULL
  ) AS nav_autocomplete_urlbar,
  IF(
    78 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(nav_visiturl_urlbar, CAST(0 AS INT64)),
    NULL
  ) AS nav_visiturl_urlbar,
  IF(
    78 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(nav_searchsuggestion_urlbar, CAST(0 AS INT64)),
    NULL
  ) AS nav_searchsuggestion_urlbar,
  IF(
    78 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(nav_topsite_urlbar, CAST(0 AS INT64)),
    NULL
  ) AS nav_topsite_urlbar,
  IF(
    38 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(num_passwords_saved, CAST(0 AS INT64)),
    NULL
  ) AS num_passwords_saved,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(unique_preferences_accessed_count, CAST(0 AS INT64)),
    NULL
  ) AS unique_preferences_accessed_count,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(preferences_accessed_total, CAST(0 AS INT64)),
    NULL
  ) AS preferences_accessed_total,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(unique_bookmarks_bar_accessed_count, CAST(0 AS INT64)),
    NULL
  ) AS unique_bookmarks_bar_accessed_count,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(bookmarks_bar_accessed_total, CAST(0 AS INT64)),
    NULL
  ) AS bookmarks_bar_accessed_total,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(unique_keyboard_shortcut_count, CAST(0 AS INT64)),
    NULL
  ) AS unique_keyboard_shortcut_count,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(keyboard_shortcut_total, CAST(0 AS INT64)),
    NULL
  ) AS keyboard_shortcut_total,
  IF(
    70 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pip_count, CAST(0 AS INT64)),
    NULL
  ) AS pip_count,
  IF(
    70 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(viewed_protection_report_count, CAST(0 AS INT64)),
    NULL
  ) AS viewed_protection_report_count,
  IF(
    70 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(etp_toggle_off, CAST(0 AS INT64)),
    NULL
  ) AS etp_toggle_off,
  IF(
    70 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(etp_toggle_on, CAST(0 AS INT64)),
    NULL
  ) AS etp_toggle_on,
  IF(
    70 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(protections_popup, CAST(0 AS INT64)),
    NULL
  ) AS protections_popup,
  IF(
    81 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(ccard_filled, CAST(0 AS INT64)),
    NULL
  ) AS ccard_filled,
  IF(
    81 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(ccard_saved, CAST(0 AS INT64)),
    NULL
  ) AS ccard_saved,
  IF(
    67 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(installed_extension, CAST(0 AS INT64)),
    NULL
  ) AS installed_extension,
  IF(
    67 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(installed_theme, CAST(0 AS INT64)),
    NULL
  ) AS installed_theme,
  IF(
    67 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(installed_l10n, CAST(0 AS INT64)),
    NULL
  ) AS installed_l10n,
  IF(
    78 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(used_stored_pw, CAST(0 AS INT64)),
    NULL
  ) AS used_stored_pw,
  IF(
    78 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(password_filled, CAST(0 AS INT64)),
    NULL
  ) AS password_filled,
  IF(
    80 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(password_saved, CAST(0 AS INT64)),
    NULL
  ) AS password_saved,
  IF(
    68 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pwmgr_opened, CAST(0 AS INT64)),
    NULL
  ) AS pwmgr_opened,
  IF(
    78 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pwmgr_copy_or_show_info, CAST(0 AS INT64)),
    NULL
  ) AS pwmgr_copy_or_show_info,
  IF(
    71 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pwmgr_interacted_breach, CAST(0 AS INT64)),
    NULL
  ) AS pwmgr_interacted_breach,
  IF(
    69 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(generated_password, CAST(0 AS INT64)),
    NULL
  ) AS generated_password,
  IF(
    72 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(fxa_connect, CAST(0 AS INT64)),
    NULL
  ) AS fxa_connect,
  IF(
    67 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(normandy_enrolled, CAST(0 AS INT64)),
    NULL
  ) AS normandy_enrolled,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(downloads, CAST(0 AS INT64)),
    NULL
  ) AS downloads,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(pdf_downloads, CAST(0 AS INT64)),
    NULL
  ) AS pdf_downloads,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(image_downloads, CAST(0 AS INT64)),
    NULL
  ) AS image_downloads,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(media_downloads, CAST(0 AS INT64)),
    NULL
  ) AS media_downloads,
  IF(
    79 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(msoffice_downloads, CAST(0 AS INT64)),
    NULL
  ) AS msoffice_downloads,
  IF(
    60 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(newtab_click, CAST(0 AS INT64)),
    NULL
  ) AS newtab_click,
  IF(
    60 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(bookmark_added_from_newtab, CAST(0 AS INT64)),
    NULL
  ) AS bookmark_added_from_newtab,
  IF(
    60 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(saved_to_pocket_from_newtab, CAST(0 AS INT64)),
    NULL
  ) AS saved_to_pocket_from_newtab,
  IF(
    60 < mozfun.norm.truncate_version(app_version, 'minor'),
    COALESCE(newtab_prefs_opened, CAST(0 AS INT64)),
    NULL
  ) AS newtab_prefs_opened,
  activitystream_reported_3rdparty_abouthome,
  activitystream_reported_3rdparty_aboutnewtab,
  activitystream_reported_3rdparty_both,
  activitystream_topsite_clicks,
  activitystream_highlight_clicks,
  activitystream_pocket_clicks,
  activitystream_sponsored_pocket_clicks,
  activitystream_sponsored_topsite_clicks,
  activitystream_organic_pocket_clicks,
  activitystream_organic_topsite_clicks,
  activitystream_reported_newtab_search_off,
  activitystream_reported_topsites_off,
  activitystream_reported_pocket_off,
  activitystream_reported_highlights_off,
  activitystream_reported_sponsored_topstories_off,
  activitystream_reported_sponsored_topsites_off,
  activitystream_sessions_abouthome,
  activitystream_sessions_newtab,
  activitystream_sessions_both,
  num_addblockers,
  has_notes_extension,
  has_facebook_container_extension,
  has_multiaccount_container_extension,
  has_private_relay_extension,
FROM
  all_features
