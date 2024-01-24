WITH _events_ping_distinct_client_count AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    COUNT(DISTINCT client_info.client_id) AS events_ping_distinct_client_count
  FROM
    firefox_ios.events_unnested
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date
),
client_product_feature_usage AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    /*Logins*/
    COUNTIF(event_category = 'logins' AND event_name = 'autofill_failed') AS logins_autofill_failed,
    COUNTIF(event_category = 'logins' AND event_name = 'autofilled') AS logins_autofilled,
    COUNTIF(event_category = 'logins' AND event_name = 'management_add_tapped') AS logins_management_add_tapped,
    COUNTIF(event_category = 'logins' AND event_name = 'management_logins_tapped') AS logins_management_logins_tapped,
    /*Credit Card*/
    COUNTIF(event_category = 'credit_card' AND event_name = 'autofill_failed') AS cc_autofill_failed,
    COUNTIF(event_category = 'credit_card' AND event_name = 'autofill_settings_tapped') AS cc_autofill_settings_tapped,
    COUNTIF(event_category = 'credit_card' AND event_name = 'autofill_toggle') AS cc_autofill_toggle,
    COUNTIF(event_category = 'credit_card' AND event_name = 'autofilled') AS cc_autofilled,
    COUNTIF(event_category = 'credit_card' AND event_name = 'form_detected') AS cc_form_detected,
    COUNTIF(event_category = 'credit_card' AND event_name = 'sync_toggle') AS cc_sync_toggle,
    COUNTIF(event_category = 'credit_card' AND event_name = 'autofill_prompt_dismissed') AS cc_autofill_prompt_dismissed,
    COUNTIF(event_category = 'credit_card' AND event_name = 'autofill_prompt_expanded') AS cc_autofill_prompt_expanded,
    COUNTIF(event_category = 'credit_card' AND event_name = 'autofill_prompt_shown') AS cc_autofill_prompt_shown,
    COUNTIF(event_category = 'credit_card' AND event_name = 'management_add_tapped') AS cc_management_add_tapped,
    COUNTIF(event_category = 'credit_card' AND event_name = 'management_card_tapped') AS cc_management_card_tapped,
    COUNTIF(event_category = 'credit_card' AND event_name = 'save_prompt_create') AS cc_save_prompt_create,
    COUNTIF(event_category = 'credit_card' AND event_name = 'save_prompt_shown') AS cc_save_prompt_shown,
    COUNTIF(event_category = 'credit_card' AND event_name = 'save_prompt_update') AS cc_save_prompt_update,
    /*Histroy*/
    COUNTIF(event_category = 'history' AND event_name = 'delete_tap') AS history_delete_tap,
    COUNTIF(event_category = 'history' AND event_name = 'opened') AS history_opened,
    COUNTIF(event_category = 'history' AND event_name = 'removed') AS history_removed,
    COUNTIF(event_category = 'history' AND event_name = 'removed_all') AS history_removed_all,
    COUNTIF(event_category = 'history' AND event_name = 'removed_today') AS history_removed_today,
    COUNTIF(event_category = 'history' AND event_name = 'removed_today_and_yesterday') AS history_removed_today_and_yesterday,
    COUNTIF(event_category = 'history' AND event_name = 'search_tap') AS history_search_tap,
    COUNTIF(event_category = 'history' AND event_name = 'opened_item') AS history_opened_item,
    /*FxA*/
    COUNTIF(event_category = 'sync' AND event_name = 'disconnect') AS fxa_disconnect,
    COUNTIF(event_category = 'sync' AND event_name = 'login_completed_view') AS fxa_login_completed_view,
    COUNTIF(event_category = 'sync' AND event_name = 'login_token_view') AS fxa_login_token_view,
    COUNTIF(event_category = 'sync' AND event_name = 'login_view') AS fxa_login_view,
    COUNTIF(event_category = 'sync' AND event_name = 'paired') AS fxa_paired,
    COUNTIF(event_category = 'sync' AND event_name = 'registration_code_view') AS fxa_registration_code_view,
    COUNTIF(event_category = 'sync' AND event_name = 'registration_completed_view') AS fxa_registration_completed_view,
    COUNTIF(event_category = 'sync' AND event_name = 'registration_view') AS fxa_registration_view,
    COUNTIF(event_category = 'sync' AND event_name = 'use_email') AS fxa_use_email,
    /*Privacy*/
    COUNTIF(event_category = 'preferences' AND event_name = 'private_browsing_button_tapped' AND extra.key = 'is_private' AND extra.value = 'true') AS private_browsing_button_tapped_enter_private_mode,
    COUNTIF(event_category = 'preferences' AND event_name = 'private_browsing_button_tapped') AS private_browsing_button_tapped,
    COUNTIF(event_category = 'tabs_tray' AND event_name = 'private_browsing_icon_tapped') AS private_browsing_icon_tapped,
    COUNTIF(event_category = 'app_icon' AND event_name = 'new_private_tab_tapped') AS app_icon_new_private_tab_tapped,
    COUNTIF(event_category = 'tabs_tray' AND event_name = 'new_private_tab_tapped') AS tabs_tray_new_private_tab_tapped,
    COUNTIF(event_category = 'tabs_tray' AND event_name = 'private_browsing_icon_tapped' AND extra.value = 'add') AS private_browsing_button_tapped_add,
    COUNTIF(event_category = 'tabs_tray' AND event_name = 'private_browsing_icon_tapped' AND extra.value = 'close_all_tabs') AS private_browsing_button_tapped_close_all_tabs,
    COUNTIF(event_category = 'tabs_tray' AND event_name = 'private_browsing_icon_tapped' AND extra.value = 'done') AS private_browsing_button_tapped_done,

    /*Awesomebar Location*/
    COUNTIF(event_category = 'awesomebar' AND event_name = 'drag_location_bar') AS drag_location_bar,
    COUNTIF(event_category = 'awesomebar' AND event_name = 'location' AND extra.value = 'top') AS location_top,
    COUNTIF(event_category = 'awesomebar' AND event_name = 'location' AND extra.value = 'bottom') AS location_bottom,
    /*Notification*/
    COUNTIF(event_category = 'app' AND event_name = 'notification_permission' AND extra.key = 'status' AND extra.value = 'authorized') AS notification_status_authorized,
    COUNTIF(event_category = 'app' AND event_name = 'notification_permission' AND extra.key = 'status' AND extra.value = 'notDetermined') AS notification_status_notDetermined,
    COUNTIF(event_category = 'app' AND event_name = 'notification_permission' AND extra.key = 'status' AND extra.value = 'denied') AS notification_status_denied,
    COUNTIF(event_category = 'app' AND event_name = 'notification_permission' AND extra.key = 'alert_setting' AND extra.value = 'notSupported') AS notification_alert_setting_not_supported,
    COUNTIF(event_category = 'app' AND event_name = 'notification_permission' AND extra.key = 'alert_setting' AND extra.value = 'disabled') AS notification_alert_setting_disabled,
    COUNTIF(event_category = 'app' AND event_name = 'notification_permission' AND extra.key = 'alert_setting' AND extra.value = 'enabled') AS notification_alert_setting_enabled
  FROM
    firefox_ios.events_unnested
  LEFT JOIN
    UNNEST(event_extra) AS extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_info.client_id,
    submission_date
),
product_features_agg AS (
  SELECT
    submission_date,
/*Logins*/
--autofill_failed
    SUM(logins_autofill_failed) AS logins_autofill_failed,
    COUNT(DISTINCT CASE WHEN logins_autofill_failed > 0 THEN client_id END) AS logins_autofill_failed_users,
--logins_autofilled
    SUM(logins_autofilled) AS logins_autofilled,
    COUNT(DISTINCT CASE WHEN logins_autofilled > 0 THEN client_id END) AS logins_autofilled_users,
--logins_management_add_tapped
    SUM(logins_management_add_tapped) AS logins_management_add_tapped,
    COUNT(DISTINCT CASE WHEN logins_management_add_tapped > 0 THEN client_id END) AS logins_management_add_tapped_users,
--logins_management_logins_tapped
    SUM(logins_management_logins_tapped) AS logins_management_logins_tapped,
    COUNT(DISTINCT CASE WHEN logins_management_logins_tapped > 0 THEN client_id END) AS logins_management_logins_tapped_users,
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
-- Sync Toggle
    SUM(cc_sync_toggle) AS cc_sync_toggle,
    COUNT(DISTINCT CASE WHEN cc_sync_toggle > 0 THEN client_id END) AS cc_sync_toggle_users,
--cc_autofill_prompt_dismissed
    SUM(cc_autofill_prompt_dismissed) AS cc_autofill_prompt_dismissed,
    COUNT(DISTINCT CASE WHEN cc_autofill_prompt_dismissed > 0 THEN client_id END) AS cc_autofill_prompt_dismissed_users,
--cc_autofill_prompt_expanded
    SUM(cc_autofill_prompt_expanded) AS cc_autofill_prompt_expanded,
    COUNT(DISTINCT CASE WHEN cc_autofill_prompt_expanded > 0 THEN client_id END) AS cc_autofill_prompt_expanded_users,
--cc_autofill_prompt_shown
    SUM(cc_autofill_prompt_shown) AS cc_autofill_prompt_shown,
    COUNT(DISTINCT CASE WHEN cc_autofill_prompt_shown > 0 THEN client_id END) AS cc_autofill_prompt_shown_users,
--cc_management_add_tapped
    SUM(cc_management_add_tapped) AS cc_management_add_tapped,
    COUNT(DISTINCT CASE WHEN cc_management_add_tapped > 0 THEN client_id END) AS cc_management_add_tapped_users,
--cc_management_card_tapped
    SUM(cc_management_card_tapped) AS cc_management_card_tapped,
    COUNT(DISTINCT CASE WHEN cc_management_card_tapped > 0 THEN client_id END) AS cc_management_card_tapped_users,
--cc_save_prompt_create
    SUM(cc_save_prompt_create) AS cc_save_prompt_create,
    COUNT(DISTINCT CASE WHEN cc_save_prompt_create > 0 THEN client_id END) AS cc_save_prompt_create_users,
--cc_save_prompt_shown
    SUM(cc_save_prompt_shown) AS cc_save_prompt_shown,
    COUNT(DISTINCT CASE WHEN cc_save_prompt_shown > 0 THEN client_id END) AS cc_save_prompt_shown_users,
--cc_save_prompt_update
    SUM(cc_save_prompt_update) AS cc_save_prompt_update,
    COUNT(DISTINCT CASE WHEN cc_save_prompt_update > 0 THEN client_id END) AS cc_save_prompt_update_users,
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
--history_opened_item
    SUM(history_opened_item) AS history_opened_item,
    COUNT(DISTINCT CASE WHEN history_opened_item > 0 THEN client_id END) AS history_opened_item_users,
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
-- private_browsing_button_tapped_add
    SUM(private_browsing_button_tapped_add) AS private_browsing_button_tapped_add,
    COUNT(
      DISTINCT
      CASE
        WHEN private_browsing_button_tapped_add > 0
          THEN client_id
      END
    ) AS private_browsing_button_tapped_add_users,
-- private_browsing_button_tapped_close_all_tabs
    SUM(private_browsing_button_tapped_close_all_tabs) AS private_browsing_button_tapped_close_all_tabs,
    COUNT(
      DISTINCT
      CASE
        WHEN private_browsing_button_tapped_close_all_tabs > 0
          THEN client_id
      END
    ) AS private_browsing_button_tapped_close_all_tabs_users,
-- private_browsing_button_tapped_done
    SUM(private_browsing_button_tapped_done) AS private_browsing_button_tapped_done,
    COUNT(
      DISTINCT
      CASE
        WHEN private_browsing_button_tapped_done > 0
          THEN client_id
      END
    ) AS private_browsing_button_tapped_done_users,
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
-- notification_status_authorized
    SUM(notification_status_authorized) AS notification_status_authorized,
    COUNT(
      DISTINCT
      CASE
        WHEN notification_status_authorized > 0
          THEN client_id
      END
    ) AS notification_status_authorized_users,
-- notification_status_notDetermined
    SUM(notification_status_notDetermined) AS notification_status_notDetermined,
    COUNT(
      DISTINCT
      CASE
        WHEN notification_status_notDetermined > 0
          THEN client_id
      END
    ) AS notification_status_notDetermined_users,
-- notification_status_denied
    SUM(notification_status_denied) AS notification_status_denied,
    COUNT(
      DISTINCT
      CASE
        WHEN notification_status_denied > 0
          THEN client_id
      END
    ) AS notification_status_denied_users,
-- notification_alert_setting_not_supported
    SUM(notification_alert_setting_not_supported) AS notification_alert_setting_not_supported,
    COUNT(
      DISTINCT
      CASE
        WHEN notification_alert_setting_not_supported > 0
          THEN client_id
      END
    ) AS notification_alert_setting_not_supported_users,
-- notification_alert_setting_disabled
    SUM(notification_alert_setting_disabled) AS notification_alert_setting_disabled,
    COUNT(
      DISTINCT
      CASE
        WHEN notification_alert_setting_disabled > 0
          THEN client_id
      END
    ) AS notification_alert_setting_disabled_users,
-- notification_alert_setting_enabled
    SUM(notification_alert_setting_enabled) AS notification_alert_setting_enabled,
    COUNT(
      DISTINCT
      CASE
        WHEN notification_alert_setting_enabled > 0
          THEN client_id
      END
    ) AS notification_alert_setting_enabled_users
  FROM
    client_product_feature_usage
  GROUP BY
    submission_date
)
SELECT
  submission_date,
  events_ping_distinct_client_count,
    /*Logins*/
  logins_autofill_failed,
  logins_autofill_failed_users,
  logins_autofilled,
  logins_autofilled_users,
  logins_management_add_tapped,
  logins_management_add_tapped_users,
  logins_management_logins_tapped,
  logins_management_logins_tapped_users,
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
  notification_alert_setting_not_supported,
  notification_alert_setting_disabled,
  notification_alert_setting_enabled,
    /*new credit card probes*/
  cc_autofill_prompt_dismissed,
  cc_autofill_prompt_dismissed_users,
  cc_autofill_prompt_expanded,
  cc_autofill_prompt_expanded_users,
  cc_autofill_prompt_shown,
  cc_autofill_prompt_shown_users,
  cc_management_add_tapped,
  cc_management_add_tapped_users,
  cc_management_card_tapped,
  cc_management_card_tapped_users,
  cc_save_prompt_shown,
  cc_save_prompt_shown_users,
  cc_save_prompt_update,
  cc_save_prompt_update_users,
    /*new history probes*/
  history_opened_item,
  history_opened_item_users,
    /*new privacy probes*/
  private_browsing_button_tapped_add,
  private_browsing_button_tapped_add_users,
  private_browsing_button_tapped_close_all_tabs,
  private_browsing_button_tapped_close_all_tabs_users,
  private_browsing_button_tapped_done,
  private_browsing_button_tapped_done_users,
    /*new notification probes*/
  notification_status_notDetermined_users,
  notification_status_denied_users,
  notification_alert_setting_not_supported_users,
  notification_alert_setting_disabled_users,
  notification_alert_setting_enabled_users
FROM
  _events_ping_distinct_client_count
JOIN
  product_features_agg
USING
  (submission_date)
