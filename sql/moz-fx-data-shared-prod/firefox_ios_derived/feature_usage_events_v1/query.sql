-- Query for firefox_ios_derived.feature_usage_events_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
DECLARE start_date DATE DEFAULT "2022-04-01";

WITH dau_segments AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    COUNT(DISTINCT client_info.client_id) AS dau
  FROM
    `mozdata.firefox_ios.events_unnested`
    --AND channel = 'release'
  WHERE
    DATE(submission_timestamp) >= '2023-06-23'
  GROUP BY
    1
),
product_features AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    /*Credit Card*/
    CASE
      WHEN event_category = 'credit_card'
        AND event_name = 'autofill_failed'
        THEN 1
      ELSE 0
    END AS cc_autofill_failed,
    CASE
      WHEN event_category = 'credit_card'
        AND event_name = 'autofill_settings_tapped'
        THEN 1
      ELSE 0
    END AS cc_autofill_settings_tapped,
    CASE
      WHEN event_category = 'credit_card'
        AND event_name = 'autofill_toggle'
        THEN 1
      ELSE 0
    END AS cc_autofill_toggle,
    CASE
      WHEN event_category = 'credit_card'
        AND event_name = 'autofilled'
        THEN 1
      ELSE 0
    END AS cc_autofilled,
    CASE
      WHEN event_category = 'credit_card'
        AND event_name = 'form_detected'
        THEN 1
      ELSE 0
    END AS cc_form_detected,
    CASE
      WHEN event_category = 'credit_card'
        AND event_name = 'save_prompt_create'
        THEN 1
      ELSE 0
    END AS cc_save_prompt_create,
    CASE
      WHEN event_category = 'credit_card'
        AND event_name = 'sync_toggle'
        THEN 1
      ELSE 0
    END AS cc_sync_toggle,
    /*Histroy*/
    CASE
      WHEN event_category = 'history'
        AND event_name = 'delete_tap'
        THEN 1
      ELSE 0
    END AS history_delete_tap,
    CASE
      WHEN event_category = 'history'
        AND event_name = 'opened'
        THEN 1
      ELSE 0
    END AS history_opened,
    CASE
      WHEN event_category = 'history'
        AND event_name = 'removed'
        THEN 1
      ELSE 0
    END AS history_removed,
    CASE
      WHEN event_category = 'history'
        AND event_name = 'removed_all'
        THEN 1
      ELSE 0
    END AS history_removed_all,
    CASE
      WHEN event_category = 'history'
        AND event_name = 'removed_today'
        THEN 1
      ELSE 0
    END AS history_removed_today,
    CASE
      WHEN event_category = 'history'
        AND event_name = 'removed_today_and_yesterday'
        THEN 1
      ELSE 0
    END AS history_removed_today_and_yesterday,
    CASE
      WHEN event_category = 'history'
        AND event_name = 'search_tap'
        THEN 1
      ELSE 0
    END AS history_search_tap,
    /*FxA*/
    CASE
      WHEN event_category = 'sync'
        AND event_name = 'disconnect'
        THEN 1
      ELSE 0
    END AS fxa_disconnect,
    CASE
      WHEN event_category = 'sync'
        AND event_name = 'login_completed_view'
        THEN 1
      ELSE 0
    END AS fxa_login_completed_view,
    CASE
      WHEN event_category = 'sync'
        AND event_name = 'login_token_view'
        THEN 1
      ELSE 0
    END AS fxa_login_token_view,
    CASE
      WHEN event_category = 'sync'
        AND event_name = 'login_view'
        THEN 1
      ELSE 0
    END AS fxa_login_view,
    CASE
      WHEN event_category = 'sync'
        AND event_name = 'paired'
        THEN 1
      ELSE 0
    END AS fxa_paired,
    CASE
      WHEN event_category = 'sync'
        AND event_name = 'registration_code_view'
        THEN 1
      ELSE 0
    END AS fxa_registration_code_view,
    CASE
      WHEN event_category = 'sync'
        AND event_name = 'registration_completed_view'
        THEN 1
      ELSE 0
    END AS fxa_registration_completed_view,
    CASE
      WHEN event_category = 'sync'
        AND event_name = 'registration_view'
        THEN 1
      ELSE 0
    END AS fxa_registration_view,
    CASE
      WHEN event_category = 'sync'
        AND event_name = 'use_email'
        THEN 1
      ELSE 0
    END AS fxa_use_email,
    /*Privacy*/
    CASE
      WHEN event_category = 'preferences'
        AND event_name = 'private_browsing_button_tapped'
        AND extra.key = 'is_private'
        AND extra.value = 'true'
        THEN 1
      ELSE 0
    END AS private_browsing_button_tapped_enter_private_mode,
    CASE
      WHEN event_category = 'preferences'
        AND event_name = 'private_browsing_button_tapped'
        THEN 1
      ELSE 0
    END AS private_browsing_button_tapped,
    CASE
      WHEN event_category = 'tabs_tray'
        AND event_name = 'private_browsing_icon_tapped'
        THEN 1
      ELSE 0
    END AS private_browsing_icon_tapped,
    CASE
      WHEN event_category = 'app_icon'
        AND event_name = 'new_private_tab_tapped'
        THEN 1
      ELSE 0
    END AS app_icon_new_private_tab_tapped,
    CASE
      WHEN event_category = 'tabs_tray'
        AND event_name = 'new_private_tab_tapped'
        THEN 1
      ELSE 0
    END AS tabs_tray_new_private_tab_tapped,
    /*Awesomebar Location*/
    CASE
      WHEN event_category = 'awesomebar'
        AND event_name = 'drag_location_bar'
        THEN 1
      ELSE 0
    END AS drag_location_bar,
    CASE
      WHEN event_category = 'awesomebar'
        AND event_name = 'location'
        AND extra.value = 'top'
        THEN 1
      ELSE 0
    END AS location_top,
    CASE
      WHEN event_category = 'awesomebar'
        AND event_name = 'location'
        AND extra.value = 'bottom'
        THEN 1
      ELSE 0
    END AS location_bottom,
    /*Notification*/
    CASE
      WHEN event_category = 'app'
        AND event_name = 'notification_permission'
        AND extra.key = 'status'
        AND extra.value = 'authorized'
        THEN 1
      ELSE 0
    END AS notification_status_authorized,
    CASE
      WHEN event_category = 'app'
        AND event_name = 'notification_permission'
        AND extra.key = 'status'
        AND extra.value = 'notDetermined'
        THEN 1
      ELSE 0
    END AS notification_status_notDetermined,
    CASE
      WHEN event_category = 'app'
        AND event_name = 'notification_permission'
        AND extra.key = 'status'
        AND extra.value = 'denied'
        THEN 1
      ELSE 0
    END AS notification_status_denied,
    CASE
      WHEN event_category = 'app'
        AND event_name = 'notification_permission'
        AND extra.key = 'alert_setting'
        AND extra.value = 'notSupported'
        THEN 1
      ELSE 0
    END AS notification_alert_setting_notSupported,
    CASE
      WHEN event_category = 'app'
        AND event_name = 'notification_permission'
        AND extra.key = 'alert_setting'
        AND extra.value = 'disabled'
        THEN 1
      ELSE 0
    END AS notification_alert_setting_disabled,
    CASE
      WHEN event_category = 'app'
        AND event_name = 'notification_permission'
        AND extra.key = 'alert_setting'
        AND extra.value = 'enabled'
        THEN 1
      ELSE 0
    END AS notification_alert_setting_enabled
  FROM
    `mozdata.firefox_ios.events_unnested`
  LEFT JOIN
    UNNEST(event_extra) AS extra
  WHERE
    DATE(submission_timestamp) >= start_date
),
product_features_agg AS (
  SELECT
    submission_date,
/*Credit Card*/
--autofill_failed
    SUM(cc_autofill_failed) AS cc_autofill_failed,
    COUNT(DISTINCT CASE WHEN cc_autofill_failed > 0 THEN client_id END) AS cc_autofill_failed_users,
-- Autofill Settings Tapped
    SUM(cc_autofill_settings_tapped) AS cc_autofill_settings_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN cc_autofill_settings_tapped > 0
          THEN client_id
      END
    ) AS cc_autofill_settings_tapped_users,
-- Autofill Toggle
    SUM(cc_autofill_toggle) AS cc_autofill_toggle,
    COUNT(DISTINCT CASE WHEN cc_autofill_toggle > 0 THEN client_id END) AS cc_autofill_toggle_users,
-- Autofilled
    SUM(cc_autofilled) AS cc_autofilled,
    COUNT(DISTINCT CASE WHEN cc_autofilled > 0 THEN client_id END) AS cc_autofilled_users,
-- Form Detected
    SUM(cc_form_detected) AS cc_form_detected,
    COUNT(DISTINCT CASE WHEN cc_form_detected > 0 THEN client_id END) AS cc_form_detected_users,
-- Save Prompt Create
    SUM(cc_save_prompt_create) AS cc_save_prompt_create,
    COUNT(
      DISTINCT
      CASE
        WHEN cc_save_prompt_create > 0
          THEN client_id
      END
    ) AS cc_save_prompt_create_users,
-- Sync Toggle
    SUM(cc_sync_toggle) AS cc_sync_toggle,
    COUNT(DISTINCT CASE WHEN cc_sync_toggle > 0 THEN client_id END) AS cc_sync_toggle_users,
/*History*/
--delete_tap
    SUM(history_delete_tap) AS history_delete_tap,
    COUNT(DISTINCT CASE WHEN history_delete_tap > 0 THEN client_id END) AS history_delete_tap_users,
-- Opened
    SUM(history_opened) AS history_opened,
    COUNT(DISTINCT CASE WHEN history_opened > 0 THEN client_id END) AS history_opened_users,
-- Removed
    SUM(history_removed) AS history_removed,
    COUNT(DISTINCT CASE WHEN history_removed > 0 THEN client_id END) AS history_removed_users,
-- Removed All
    SUM(history_removed_all) AS history_removed_all,
    COUNT(
      DISTINCT
      CASE
        WHEN history_removed_all > 0
          THEN client_id
      END
    ) AS history_removed_all_users,
-- Removed Today
    SUM(history_removed_today) AS history_removed_today,
    COUNT(
      DISTINCT
      CASE
        WHEN history_removed_today > 0
          THEN client_id
      END
    ) AS history_removed_today_users,
-- Removed Today and Yesterday
    SUM(history_removed_today_and_yesterday) AS history_removed_today_and_yesterday,
    COUNT(
      DISTINCT
      CASE
        WHEN history_removed_today_and_yesterday > 0
          THEN client_id
      END
    ) AS history_removed_today_and_yesterday_users,
-- Search Tap
    SUM(history_search_tap) AS history_search_tap,
    COUNT(DISTINCT CASE WHEN history_search_tap > 0 THEN client_id END) AS history_search_tap_users,
/*FxA*/
--disconnect
    SUM(fxa_disconnect) AS fxa_disconnect,
    COUNT(DISTINCT CASE WHEN fxa_disconnect > 0 THEN client_id END) AS fxa_disconnect_users,
 -- Login Completed View
    SUM(fxa_login_completed_view) AS fxa_login_completed_view,
    COUNT(
      DISTINCT
      CASE
        WHEN fxa_login_completed_view > 0
          THEN client_id
      END
    ) AS fxa_login_completed_view_users,
-- Login Token View
    SUM(fxa_login_token_view) AS fxa_login_token_view,
    COUNT(
      DISTINCT
      CASE
        WHEN fxa_login_token_view > 0
          THEN client_id
      END
    ) AS fxa_login_token_view_users,
-- Login View
    SUM(fxa_login_view) AS fxa_login_view,
    COUNT(DISTINCT CASE WHEN fxa_login_view > 0 THEN client_id END) AS fxa_login_view_users,
-- Paired
    SUM(fxa_paired) AS fxa_paired,
    COUNT(DISTINCT CASE WHEN fxa_paired > 0 THEN client_id END) AS fxa_paired_users,
-- Registration Code View
    SUM(fxa_registration_code_view) AS fxa_registration_code_view,
    COUNT(
      DISTINCT
      CASE
        WHEN fxa_registration_code_view > 0
          THEN client_id
      END
    ) AS fxa_registration_code_view_users,
-- Registration Completed View
    SUM(fxa_registration_completed_view) AS fxa_registration_completed_view,
    COUNT(
      DISTINCT
      CASE
        WHEN fxa_registration_completed_view > 0
          THEN client_id
      END
    ) AS fxa_registration_completed_view_users,
-- Registration View
    SUM(fxa_registration_view) AS fxa_registration_view,
    COUNT(
      DISTINCT
      CASE
        WHEN fxa_registration_view > 0
          THEN client_id
      END
    ) AS fxa_registration_view_users,
-- Use Email
    SUM(fxa_use_email) AS fxa_use_email,
    COUNT(DISTINCT CASE WHEN fxa_use_email > 0 THEN client_id END) AS fxa_use_email_users,
/*Privacy*/
--private_browsing_button_tapped
    SUM(private_browsing_button_tapped) AS private_browsing_button_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN private_browsing_button_tapped > 0
          THEN client_id
      END
    ) AS private_browsing_button_tapped_users,
--private_browsing_button_tapped_enter_private_mode
    SUM(
      private_browsing_button_tapped_enter_private_mode
    ) AS private_browsing_button_tapped_enter_private_mode,
    COUNT(
      DISTINCT
      CASE
        WHEN private_browsing_button_tapped_enter_private_mode > 0
          THEN client_id
      END
    ) AS private_browsing_button_tapped_enter_private_mode_users,
-- Private Browsing Icon Tapped
    SUM(private_browsing_icon_tapped) AS private_browsing_icon_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN private_browsing_icon_tapped > 0
          THEN client_id
      END
    ) AS private_browsing_icon_tapped_users,
-- App Icon New Private Tab Tapped
    SUM(app_icon_new_private_tab_tapped) AS app_icon_new_private_tab_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN app_icon_new_private_tab_tapped > 0
          THEN client_id
      END
    ) AS app_icon_new_private_tab_tapped_users,
-- Tabs Tray New Private Tab Tapped
    SUM(tabs_tray_new_private_tab_tapped) AS tabs_tray_new_private_tab_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN tabs_tray_new_private_tab_tapped > 0
          THEN client_id
      END
    ) AS tabs_tray_new_private_tab_tapped_users,
/*Awesomebar Location*/
--drag_location_bar
    SUM(drag_location_bar) AS drag_location_bar,
    COUNT(DISTINCT CASE WHEN drag_location_bar > 0 THEN client_id END) AS drag_location_bar_users,
-- location_top
    SUM(location_top) AS location_top,
    COUNT(DISTINCT CASE WHEN location_top > 0 THEN client_id END) AS location_top_users,
-- location_bottom
    SUM(location_bottom) AS location_bottom,
    COUNT(DISTINCT CASE WHEN location_bottom > 0 THEN client_id END) AS location_bottom_users,
/*Notification*/
    SUM(notification_status_authorized) AS notification_status_authorized,
    COUNT(
      DISTINCT
      CASE
        WHEN notification_status_authorized > 0
          THEN client_id
      END
    ) AS notification_status_authorized_users,
    SUM(notification_status_notDetermined) AS notification_status_notDetermined,
    SUM(notification_status_denied) AS notification_status_denied,
    SUM(notification_alert_setting_notSupported) AS notification_alert_setting_notSupported,
    SUM(notification_alert_setting_disabled) AS notification_alert_setting_disabled,
    SUM(notification_alert_setting_enabled) AS notification_alert_setting_enabled
  FROM
    product_features
  WHERE
    submission_date >= start_date
  GROUP BY
    1
)
SELECT
  d.submission_date,
  dau,
    /*Credit Card*/
  cc_autofill_failed,
  cc_autofill_failed_users,
  cc_autofill_settings_tapped,
  cc_autofill_settings_tapped_users,
  cc_autofill_toggle,
  cc_autofill_toggle_users,
  cc_autofilled,
  cc_autofilled_users,
  cc_form_detected,
  cc_form_detected_users,
  cc_save_prompt_create,
  cc_save_prompt_create_users,
  cc_sync_toggle,
  cc_sync_toggle_users,
    /*History*/
  history_delete_tap,
  history_delete_tap_users,
  history_opened,
  history_opened_users,
  history_removed,
  history_removed_users,
  history_removed_all,
  history_removed_all_users,
  history_removed_today,
  history_removed_today_users,
  history_removed_today_and_yesterday,
  history_removed_today_and_yesterday_users,
  history_search_tap,
  history_search_tap_users,
    /*FxA*/
  fxa_disconnect,
  fxa_disconnect_users,
  fxa_login_completed_view,
  fxa_login_completed_view_users,
  fxa_login_token_view,
  fxa_login_token_view_users,
  fxa_login_view,
  fxa_login_view_users,
  fxa_paired,
  fxa_paired_users,
  fxa_registration_code_view,
  fxa_registration_code_view_users,
  fxa_registration_completed_view,
  fxa_registration_completed_view_users,
  fxa_registration_view,
  fxa_registration_view_users,
  fxa_use_email,
  fxa_use_email_users,
    /*Privacy*/
  private_browsing_button_tapped,
  private_browsing_button_tapped_users,
  private_browsing_button_tapped_enter_private_mode,
  private_browsing_button_tapped_enter_private_mode_users,
  private_browsing_icon_tapped,
  private_browsing_icon_tapped_users,
  app_icon_new_private_tab_tapped,
  app_icon_new_private_tab_tapped_users,
  tabs_tray_new_private_tab_tapped,
  tabs_tray_new_private_tab_tapped_users,
    /*Awesomebar Location*/
  drag_location_bar,
  drag_location_bar_users,
  location_top,
  location_top_users,
  location_bottom,
  location_bottom_users,
    /*Notification*/
  notification_status_authorized,
  notification_status_authorized_users,
  notification_status_notDetermined,
  notification_status_denied,
  notification_alert_setting_notSupported,
  notification_alert_setting_disabled,
  notification_alert_setting_enabled
FROM
  dau_segments d
LEFT JOIN
  product_features_agg p
ON
  d.submission_date = p.submission_date
ORDER BY
  d.submission_date
