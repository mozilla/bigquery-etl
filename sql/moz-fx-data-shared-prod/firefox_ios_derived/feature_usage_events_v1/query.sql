WITH baseline_clients AS (
  SELECT
    DATE(
      DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')
    ) AS ping_date,
    client_info.client_id,
    normalized_channel AS channel,
    normalized_country_code AS country
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline`
  WHERE
    metrics.timespan.glean_baseline_duration.value > 0
    AND LOWER(metadata.isp.name) <> "browserstack"
    AND DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 4 DAY)
    AND @submission_date
    AND DATE(
      DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')
    ) = DATE_SUB(@submission_date, INTERVAL 4 DAY)
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        client_info.client_id
      ORDER BY
        DATE(DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC'))
    ) = 1
),
client_attribution AS (
  SELECT
    client_id,
    adjust_network,
    channel,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.attribution_clients`
),
default_browser AS (
  SELECT
    -- In rare cases we can have an end_time that is earlier than the start_time, we made the decision
    -- to attribute the metrics to the earlier date of the two.
    DATE(
      DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')
    ) AS ping_date,
    client_info.client_id,
    normalized_channel AS channel,
    normalized_country_code AS country,
    IF(SUM(metrics.counter.app_opened_as_default_browser) > 0, TRUE, FALSE) AS is_default_browser
  FROM
    `moz-fx-data-shared-prod.firefox_ios.metrics` AS metric_ping
  WHERE
    LOWER(metadata.isp.name) <> "browserstack"
    -- we need to work with a larger time window as some metrics ping arrive with a multi day delay
    AND DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 4 DAY)
    AND @submission_date
    AND DATE(
      DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')
    ) = DATE_SUB(@submission_date, INTERVAL 4 DAY)
  GROUP BY
    ping_date,
    client_id,
    channel,
    country
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY ping_date) = 1
),
event_ping_clients_feature_usage AS (
  SELECT
    DATE(
      DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')
    ) AS ping_date,
    client_info.client_id,
    normalized_channel AS channel,
    normalized_country_code AS country,
    /*Logins*/
    COUNTIF(event_category = 'logins' AND event_name = 'autofill_failed') AS logins_autofill_failed,
    COUNTIF(event_category = 'logins' AND event_name = 'autofilled') AS logins_autofilled,
    COUNTIF(
      event_category = 'logins'
      AND event_name = 'management_add_tapped'
    ) AS logins_management_add_tapped,
    COUNTIF(
      event_category = 'logins'
      AND event_name = 'management_logins_tapped'
    ) AS logins_management_logins_tapped,
      /*Credit Card*/
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'autofill_failed'
    ) AS cc_autofill_failed,
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'autofill_settings_tapped'
    ) AS cc_autofill_settings_tapped,
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'autofill_toggle'
    ) AS cc_autofill_toggle,
    COUNTIF(event_category = 'credit_card' AND event_name = 'autofilled') AS cc_autofilled,
    COUNTIF(event_category = 'credit_card' AND event_name = 'form_detected') AS cc_form_detected,
    COUNTIF(event_category = 'credit_card' AND event_name = 'sync_toggle') AS cc_sync_toggle,
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'autofill_prompt_dismissed'
    ) AS cc_autofill_prompt_dismissed,
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'autofill_prompt_expanded'
    ) AS cc_autofill_prompt_expanded,
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'autofill_prompt_shown'
    ) AS cc_autofill_prompt_shown,
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'management_add_tapped'
    ) AS cc_management_add_tapped,
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'management_card_tapped'
    ) AS cc_management_card_tapped,
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'save_prompt_create'
    ) AS cc_save_prompt_create,
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'save_prompt_shown'
    ) AS cc_save_prompt_shown,
    COUNTIF(
      event_category = 'credit_card'
      AND event_name = 'save_prompt_update'
    ) AS cc_save_prompt_update,
      /*Histroy*/
    COUNTIF(event_category = 'history' AND event_name = 'delete_tap') AS history_delete_tap,
    COUNTIF(event_category = 'history' AND event_name = 'opened') AS history_opened,
    COUNTIF(event_category = 'history' AND event_name = 'removed') AS history_removed,
    COUNTIF(event_category = 'history' AND event_name = 'removed_all') AS history_removed_all,
    COUNTIF(event_category = 'history' AND event_name = 'removed_today') AS history_removed_today,
    COUNTIF(
      event_category = 'history'
      AND event_name = 'removed_today_and_yesterday'
    ) AS history_removed_today_and_yesterday,
    COUNTIF(event_category = 'history' AND event_name = 'search_tap') AS history_search_tap,
    COUNTIF(event_category = 'history' AND event_name = 'opened_item') AS history_opened_item,
      /*FxA*/
    COUNTIF(event_category = 'sync' AND event_name = 'disconnect') AS fxa_disconnect,
    COUNTIF(
      event_category = 'sync'
      AND event_name = 'login_completed_view'
    ) AS fxa_login_completed_view,
    COUNTIF(event_category = 'sync' AND event_name = 'login_token_view') AS fxa_login_token_view,
    COUNTIF(event_category = 'sync' AND event_name = 'login_view') AS fxa_login_view,
    COUNTIF(event_category = 'sync' AND event_name = 'paired') AS fxa_paired,
    COUNTIF(
      event_category = 'sync'
      AND event_name = 'registration_code_view'
    ) AS fxa_registration_code_view,
    COUNTIF(
      event_category = 'sync'
      AND event_name = 'registration_completed_view'
    ) AS fxa_registration_completed_view,
    COUNTIF(event_category = 'sync' AND event_name = 'registration_view') AS fxa_registration_view,
    COUNTIF(event_category = 'sync' AND event_name = 'use_email') AS fxa_use_email,
      /*Privacy*/
    COUNTIF(
      event_category = 'preferences'
      AND event_name = 'private_browsing_button_tapped'
      AND extra.key = 'is_private'
      AND extra.value = 'true'
    ) AS private_browsing_button_tapped_enter_private_mode,
    COUNTIF(
      event_category = 'preferences'
      AND event_name = 'private_browsing_button_tapped'
    ) AS private_browsing_button_tapped,
    COUNTIF(
      event_category = 'tabs_tray'
      AND event_name = 'private_browsing_icon_tapped'
    ) AS private_browsing_icon_tapped,
    COUNTIF(
      event_category = 'app_icon'
      AND event_name = 'new_private_tab_tapped'
    ) AS app_icon_new_private_tab_tapped,
    COUNTIF(
      event_category = 'tabs_tray'
      AND event_name = 'new_private_tab_tapped'
    ) AS tabs_tray_new_private_tab_tapped,
    COUNTIF(
      event_category = 'tabs_tray'
      AND event_name = 'private_browsing_icon_tapped'
      AND extra.value = 'add'
    ) AS private_browsing_button_tapped_add,
    COUNTIF(
      event_category = 'tabs_tray'
      AND event_name = 'private_browsing_icon_tapped'
      AND extra.value = 'close_all_tabs'
    ) AS private_browsing_button_tapped_close_all_tabs,
    COUNTIF(
      event_category = 'tabs_tray'
      AND event_name = 'private_browsing_icon_tapped'
      AND extra.value = 'done'
    ) AS private_browsing_button_tapped_done,
      /*Awesomebar Location*/
    COUNTIF(
      event_category = 'awesomebar'
      AND event_name = 'drag_location_bar'
    ) AS drag_location_bar,
    COUNTIF(
      event_category = 'awesomebar'
      AND event_name = 'location'
      AND extra.value = 'top'
    ) AS location_top,
    COUNTIF(
      event_category = 'awesomebar'
      AND event_name = 'location'
      AND extra.value = 'bottom'
    ) AS location_bottom,
      /*Notification*/
    COUNTIF(
      event_category = 'app'
      AND event_name = 'notification_permission'
      AND extra.key = 'status'
      AND extra.value = 'authorized'
    ) AS notification_status_authorized,
    COUNTIF(
      event_category = 'app'
      AND event_name = 'notification_permission'
      AND extra.key = 'status'
      AND extra.value = 'notDetermined'
    ) AS notification_status_notDetermined,
    COUNTIF(
      event_category = 'app'
      AND event_name = 'notification_permission'
      AND extra.key = 'status'
      AND extra.value = 'denied'
    ) AS notification_status_denied,
    COUNTIF(
      event_category = 'app'
      AND event_name = 'notification_permission'
      AND extra.key = 'alert_setting'
      AND extra.value = 'notSupported'
    ) AS notification_alert_setting_not_supported,
    COUNTIF(
      event_category = 'app'
      AND event_name = 'notification_permission'
      AND extra.key = 'alert_setting'
      AND extra.value = 'disabled'
    ) AS notification_alert_setting_disabled,
    COUNTIF(
      event_category = 'app'
      AND event_name = 'notification_permission'
      AND extra.key = 'alert_setting'
      AND extra.value = 'enabled'
    ) AS notification_alert_setting_enabled,
      /*Address*/
    COUNTIF(
      event_category = 'addresses'
      AND event_name = 'autofill_prompt_dismissed'
    ) AS address_autofill_prompt_dismissed,
    COUNTIF(
      event_category = 'addresses'
      AND event_name = 'autofill_prompt_expanded'
    ) AS address_autofill_prompt_expanded,
    COUNTIF(
      event_category = 'addresses'
      AND event_name = 'autofill_prompt_shown'
    ) AS address_autofill_prompt_shown,
    COUNTIF(event_category = 'addresses' AND event_name = 'autofilled') AS address_autofilled,
    COUNTIF(event_category = 'addresses' AND event_name = 'form_detected') AS address_form_detected,
    COUNTIF(event_category = 'addresses' AND event_name = 'modified') AS address_modified,
    COUNTIF(
      event_category = 'addresses'
      AND event_name = 'settings_autofill'
    ) AS address_settings_autofill
  FROM
    `moz-fx-data-shared-prod.firefox_ios.events_unnested`
  LEFT JOIN
    UNNEST(event_extra) AS extra
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 4 DAY)
    AND @submission_date
    AND DATE(
      DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')
    ) = DATE_SUB(@submission_date, INTERVAL 4 DAY)
  GROUP BY
    ping_date,
    client_id,
    channel,
    country
)
SELECT
  @submission_date AS submission_date,
  ping_date,
  channel,
  country,
  adjust_network,
  is_default_browser,
/*Logins*/
--autofill_failed
  COUNT(DISTINCT IF(logins_autofill_failed > 0, client_id, NULL)) AS logins_autofill_failed_users,
  SUM(logins_autofill_failed) AS logins_autofill_failed,
--logins_autofilled
  COUNT(DISTINCT IF(logins_autofilled > 0, client_id, NULL)) AS logins_autofilled_users,
  SUM(logins_autofilled) AS logins_autofilled,
--logins_management_add_tapped
  COUNT(
    DISTINCT IF(logins_management_add_tapped > 0, client_id, NULL)
  ) AS logins_management_add_tapped_users,
  SUM(logins_management_add_tapped) AS logins_management_add_tapped,
--logins_management_logins_tapped
  COUNT(
    DISTINCT IF(logins_management_logins_tapped > 0, client_id, NULL)
  ) AS logins_management_logins_tapped_users,
  SUM(logins_management_logins_tapped) AS logins_management_logins_tapped,
/*Credit Card*/
--autofill_failed
  COUNT(DISTINCT IF(cc_autofill_failed > 0, client_id, NULL)) AS cc_autofill_failed_users,
  SUM(cc_autofill_failed) AS cc_autofill_failed,
-- Autofill Settings Tapped
  COUNT(
    DISTINCT IF(cc_autofill_settings_tapped > 0, client_id, NULL)
  ) AS cc_autofill_settings_tapped_users,
  SUM(cc_autofill_settings_tapped) AS cc_autofill_settings_tapped,
-- Autofill Toggle
  COUNT(DISTINCT IF(cc_autofill_toggle > 0, client_id, NULL)) AS cc_autofill_toggle_users,
  SUM(cc_autofill_toggle) AS cc_autofill_toggle,
-- Autofilled
  COUNT(DISTINCT IF(cc_autofilled > 0, client_id, NULL)) AS cc_autofilled_users,
  SUM(cc_autofilled) AS cc_autofilled,
-- Form Detected
  COUNT(DISTINCT IF(cc_form_detected > 0, client_id, NULL)) AS cc_form_detected_users,
  SUM(cc_form_detected) AS cc_form_detected,
-- Sync Toggle
  COUNT(DISTINCT IF(cc_sync_toggle > 0, client_id, NULL)) AS cc_sync_toggle_users,
  SUM(cc_sync_toggle) AS cc_sync_toggle,
--cc_autofill_prompt_dismissed
  COUNT(
    DISTINCT IF(cc_autofill_prompt_dismissed > 0, client_id, NULL)
  ) AS cc_autofill_prompt_dismissed_users,
  SUM(cc_autofill_prompt_dismissed) AS cc_autofill_prompt_dismissed,
--cc_autofill_prompt_expanded
  COUNT(
    DISTINCT IF(cc_autofill_prompt_expanded > 0, client_id, NULL)
  ) AS cc_autofill_prompt_expanded_users,
  SUM(cc_autofill_prompt_expanded) AS cc_autofill_prompt_expanded,
--cc_autofill_prompt_shown
  COUNT(
    DISTINCT IF(cc_autofill_prompt_shown > 0, client_id, NULL)
  ) AS cc_autofill_prompt_shown_users,
  SUM(cc_autofill_prompt_shown) AS cc_autofill_prompt_shown,
--cc_management_add_tapped
  COUNT(
    DISTINCT IF(cc_management_add_tapped > 0, client_id, NULL)
  ) AS cc_management_add_tapped_users,
  SUM(cc_management_add_tapped) AS cc_management_add_tapped,
--cc_management_card_tapped
  COUNT(
    DISTINCT IF(cc_management_card_tapped > 0, client_id, NULL)
  ) AS cc_management_card_tapped_users,
  SUM(cc_management_card_tapped) AS cc_management_card_tapped,
--cc_save_prompt_create
  COUNT(DISTINCT IF(cc_save_prompt_create > 0, client_id, NULL)) AS cc_save_prompt_create_users,
  SUM(cc_save_prompt_create) AS cc_save_prompt_create,
--cc_save_prompt_shown
  COUNT(DISTINCT IF(cc_save_prompt_shown > 0, client_id, NULL)) AS cc_save_prompt_shown_users,
  SUM(cc_save_prompt_shown) AS cc_save_prompt_shown,
--cc_save_prompt_update
  COUNT(DISTINCT IF(cc_save_prompt_update > 0, client_id, NULL)) AS cc_save_prompt_update_users,
  SUM(cc_save_prompt_update) AS cc_save_prompt_update,
/*History*/
--delete_tap
  COUNT(DISTINCT IF(history_delete_tap > 0, client_id, NULL)) AS history_delete_tap_users,
  SUM(history_delete_tap) AS history_delete_tap,
-- Opened
  COUNT(DISTINCT IF(history_opened > 0, client_id, NULL)) AS history_opened_users,
  SUM(history_opened) AS history_opened,
-- Removed
  COUNT(DISTINCT IF(history_removed > 0, client_id, NULL)) AS history_removed_users,
  SUM(history_removed) AS history_removed,
-- Removed All
  COUNT(DISTINCT IF(history_removed_all > 0, client_id, NULL)) AS history_removed_all_users,
  SUM(history_removed_all) AS history_removed_all,
-- Removed Today
  COUNT(DISTINCT IF(history_removed_today > 0, client_id, NULL)) AS history_removed_today_users,
  SUM(history_removed_today) AS history_removed_today,
-- Removed Today and Yesterday
  COUNT(
    DISTINCT IF(history_removed_today_and_yesterday > 0, client_id, NULL)
  ) AS history_removed_today_and_yesterday_users,
  SUM(history_removed_today_and_yesterday) AS history_removed_today_and_yesterday,
-- Search Tap
  COUNT(DISTINCT IF(history_search_tap > 0, client_id, NULL)) AS history_search_tap_users,
  SUM(history_search_tap) AS history_search_tap,
--history_opened_item
  COUNT(DISTINCT IF(history_opened_item > 0, client_id, NULL)) AS history_opened_item_users,
  SUM(history_opened_item) AS history_opened_item,
/*FxA*/
--disconnect
  COUNT(DISTINCT IF(fxa_disconnect > 0, client_id, NULL)) AS fxa_disconnect_users,
  SUM(fxa_disconnect) AS fxa_disconnect,
 -- Login Completed View
  COUNT(
    DISTINCT IF(fxa_login_completed_view > 0, client_id, NULL)
  ) AS fxa_login_completed_view_users,
  SUM(fxa_login_completed_view) AS fxa_login_completed_view,
-- Login Token View
  COUNT(DISTINCT IF(fxa_login_token_view > 0, client_id, NULL)) AS fxa_login_token_view_users,
  SUM(fxa_login_token_view) AS fxa_login_token_view,
-- Login View
  COUNT(DISTINCT IF(fxa_login_view > 0, client_id, NULL)) AS fxa_login_view_users,
  SUM(fxa_login_view) AS fxa_login_view,
-- Paired
  COUNT(DISTINCT IF(fxa_paired > 0, client_id, NULL)) AS fxa_paired_users,
  SUM(fxa_paired) AS fxa_paired,
-- Registration Code View
  COUNT(
    DISTINCT IF(fxa_registration_code_view > 0, client_id, NULL)
  ) AS fxa_registration_code_view_users,
  SUM(fxa_registration_code_view) AS fxa_registration_code_view,
-- Registration Completed View
  COUNT(
    DISTINCT IF(fxa_registration_completed_view > 0, client_id, NULL)
  ) AS fxa_registration_completed_view_users,
  SUM(fxa_registration_completed_view) AS fxa_registration_completed_view,
-- Registration View
  COUNT(DISTINCT IF(fxa_registration_view > 0, client_id, NULL)) AS fxa_registration_view_users,
  SUM(fxa_registration_view) AS fxa_registration_view,
-- Use Email
  COUNT(DISTINCT IF(fxa_use_email > 0, client_id, NULL)) AS fxa_use_email_users,
  SUM(fxa_use_email) AS fxa_use_email,
/*Privacy*/
--private_browsing_button_tapped
  COUNT(
    DISTINCT IF(private_browsing_button_tapped > 0, client_id, NULL)
  ) AS private_browsing_button_tapped_users,
  SUM(private_browsing_button_tapped) AS private_browsing_button_tapped,
--private_browsing_button_tapped_enter_private_mode
  COUNT(
    DISTINCT IF(private_browsing_button_tapped_enter_private_mode > 0, client_id, NULL)
  ) AS private_browsing_button_tapped_enter_private_mode_users,
  SUM(
    private_browsing_button_tapped_enter_private_mode
  ) AS private_browsing_button_tapped_enter_private_mode,
-- Private Browsing Icon Tapped
  COUNT(
    DISTINCT IF(private_browsing_icon_tapped > 0, client_id, NULL)
  ) AS private_browsing_icon_tapped_users,
  SUM(private_browsing_icon_tapped) AS private_browsing_icon_tapped,
-- App Icon New Private Tab Tapped
  COUNT(
    DISTINCT IF(app_icon_new_private_tab_tapped > 0, client_id, NULL)
  ) AS app_icon_new_private_tab_tapped_users,
  SUM(app_icon_new_private_tab_tapped) AS app_icon_new_private_tab_tapped,
-- Tabs Tray New Private Tab Tapped
  COUNT(
    DISTINCT IF(tabs_tray_new_private_tab_tapped > 0, client_id, NULL)
  ) AS tabs_tray_new_private_tab_tapped_users,
  SUM(tabs_tray_new_private_tab_tapped) AS tabs_tray_new_private_tab_tapped,
-- private_browsing_button_tapped_add
  COUNT(
    DISTINCT IF(private_browsing_button_tapped_add > 0, client_id, NULL)
  ) AS private_browsing_button_tapped_add_users,
  SUM(private_browsing_button_tapped_add) AS private_browsing_button_tapped_add,
-- private_browsing_button_tapped_close_all_tabs
  COUNT(
    DISTINCT IF(private_browsing_button_tapped_close_all_tabs > 0, client_id, NULL)
  ) AS private_browsing_button_tapped_close_all_tabs_users,
  SUM(
    private_browsing_button_tapped_close_all_tabs
  ) AS private_browsing_button_tapped_close_all_tabs,
-- private_browsing_button_tapped_done
  COUNT(
    DISTINCT IF(private_browsing_button_tapped_done > 0, client_id, NULL)
  ) AS private_browsing_button_tapped_done_users,
  SUM(private_browsing_button_tapped_done) AS private_browsing_button_tapped_done,
/*Awesomebar Location*/
--drag_location_bar
  COUNT(DISTINCT IF(drag_location_bar > 0, client_id, NULL)) AS drag_location_bar_users,
  SUM(drag_location_bar) AS drag_location_bar,
-- location_top
  COUNT(DISTINCT IF(location_top > 0, client_id, NULL)) AS location_top_users,
  SUM(location_top) AS location_top,
-- location_bottom
  COUNT(DISTINCT IF(location_bottom > 0, client_id, NULL)) AS location_bottom_users,
  SUM(location_bottom) AS location_bottom,
/*Notification*/
-- notification_status_authorized
  COUNT(
    DISTINCT IF(notification_status_authorized > 0, client_id, NULL)
  ) AS notification_status_authorized_users,
  SUM(notification_status_authorized) AS notification_status_authorized,
-- notification_status_notDetermined
  COUNT(
    DISTINCT IF(notification_status_notDetermined > 0, client_id, NULL)
  ) AS notification_status_notDetermined_users,
  SUM(notification_status_notDetermined) AS notification_status_notDetermined,
-- notification_status_denied
  COUNT(
    DISTINCT IF(notification_status_denied > 0, client_id, NULL)
  ) AS notification_status_denied_users,
  SUM(notification_status_denied) AS notification_status_denied,
-- notification_alert_setting_not_supported
  COUNT(
    DISTINCT IF(notification_alert_setting_not_supported > 0, client_id, NULL)
  ) AS notification_alert_setting_not_supported_users,
  SUM(notification_alert_setting_not_supported) AS notification_alert_setting_not_supported,
-- notification_alert_setting_disabled
  COUNT(
    DISTINCT IF(notification_alert_setting_disabled > 0, client_id, NULL)
  ) AS notification_alert_setting_disabled_users,
  SUM(notification_alert_setting_disabled) AS notification_alert_setting_disabled,
-- notification_alert_setting_enabled
  COUNT(
    DISTINCT IF(notification_alert_setting_enabled > 0, client_id, NULL)
  ) AS notification_alert_setting_enabled_users,
  SUM(notification_alert_setting_enabled) AS notification_alert_setting_enabled,
-- address_autofill_prompt_dismissed
  COUNT(
    DISTINCT IF(address_autofill_prompt_dismissed > 0, client_id, NULL)
  ) AS address_autofill_prompt_dismissed_users,
  SUM(address_autofill_prompt_dismissed) AS address_autofill_prompt_dismissed,
-- address_autofill_prompt_expanded
  COUNT(
    DISTINCT IF(address_autofill_prompt_expanded > 0, client_id, NULL)
  ) AS address_autofill_prompt_expanded_users,
  SUM(address_autofill_prompt_expanded) AS address_autofill_prompt_expanded,
-- address_autofill_prompt_shown
  COUNT(
    DISTINCT IF(address_autofill_prompt_shown > 0, client_id, NULL)
  ) AS address_autofill_prompt_shown_users,
  SUM(address_autofill_prompt_shown) AS address_autofill_prompt_shown,
-- address_autofilled
  COUNT(DISTINCT IF(address_autofilled > 0, client_id, NULL)) AS address_autofilled_users,
  SUM(address_autofilled) AS address_autofilled,
-- address_form_detected
  COUNT(DISTINCT IF(address_form_detected > 0, client_id, NULL)) AS address_form_detected_users,
  SUM(address_form_detected) AS address_form_detected,
-- address_modified
  COUNT(DISTINCT IF(address_modified > 0, client_id, NULL)) AS address_modified_users,
  SUM(address_modified) AS address_modified,
-- address_settings_autofill
  COUNT(
    DISTINCT IF(address_settings_autofill > 0, client_id, NULL)
  ) AS address_settings_autofill_users,
  SUM(address_settings_autofill) AS address_settings_autofill
FROM
  event_ping_clients_feature_usage
INNER JOIN
  baseline_clients
  USING (ping_date, client_id, channel, country)
LEFT JOIN
  client_attribution
  USING (client_id, channel)
LEFT JOIN
  default_browser
  USING (ping_date, client_id, channel, country)
GROUP BY
  submission_date,
  ping_date,
  channel,
  country,
  adjust_network,
  is_default_browser
