WITH baseline_clients AS (
  SELECT DISTINCT
    DATE(DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')) AS `date`,
    client_info.client_id,
    normalized_channel AS channel,
    normalized_country_code AS country,
  FROM firefox_ios.baseline
  WHERE
    metrics.timespan.glean_baseline_duration.value > 0
    AND LOWER(metadata.isp.name) <> "browserstack"
    AND DATE(submission_timestamp) BETWEEN DATE_SUB(@submission_date, INTERVAL 4 DAY) AND @submission_date
    AND DATE(DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')) = DATE_SUB(@submission_date, INTERVAL 4 DAY)
),
client_attribution AS (
  SELECT
    client_id,
    channel,
    adjust_network,
  FROM firefox_ios.firefox_ios_clients
),
metric_ping_clients_feature_usage AS (
  SELECT
    -- In rare cases we can have an end_time that is earlier than the start_time, we made the decision
    -- to attribute the metrics to the earlier date of the two.
    DATE(DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')) AS `date`,
    client_info.client_id,
    normalized_channel AS channel,
    normalized_country_code AS country,
    IF(SUM(metrics.counter.app_opened_as_default_browser) > 0, TRUE, FALSE) AS is_default_browser,
    --Credential Management: Logins
    SUM(COALESCE(metrics.counter.logins_deleted, 0)) AS logins_deleted,
    SUM(COALESCE(metrics.counter.logins_modified, 0)) AS logins_modified,
    SUM(COALESCE(metrics.counter.logins_saved, 0)) AS logins_saved,
    SUM(COALESCE(metrics.quantity.logins_saved_all, 0)) AS logins_saved_all,
    --Credential Management: Credit Cards
    COUNTIF(metrics.boolean.credit_card_autofill_enabled) AS credit_card_autofill_enabled,
    COUNTIF(metrics.boolean.credit_card_sync_enabled) AS credit_card_sync_enabled,
    SUM(COALESCE(metrics.counter.credit_card_deleted, 0)) AS credit_card_deleted,
    SUM(COALESCE(metrics.counter.credit_card_modified, 0)) AS credit_card_modified,
    SUM(COALESCE(metrics.counter.credit_card_saved, 0)) AS credit_card_saved,
    SUM(COALESCE(metrics.quantity.credit_card_saved_all, 0)) AS credit_card_saved_all,
    --Bookmark
    SUM(COALESCE(bookmarks_add_table.value, 0)) AS bookmarks_add,
    SUM(COALESCE(bookmarks_delete_table.value, 0)) AS bookmarks_delete,
    SUM(COALESCE(bookmarks_edit_table.value, 0)) AS bookmarks_edit,
    COUNTIF(metrics.boolean.bookmarks_has_mobile_bookmarks) AS has_mobile_bookmarks,
    SUM(COALESCE(metrics.quantity.bookmarks_mobile_bookmarks_count, 0)) AS mobile_bookmarks_count,
    SUM(COALESCE(bookmarks_open_table.value, 0)) AS bookmarks_open,
    SUM(COALESCE(bookmarks_view_list_table.value, 0)) AS bookmarks_view_list,
    --FxA
    SUM(COALESCE(metrics.counter.sync_create_account_pressed, 0)) AS sync_create_account_pressed,
    SUM(COALESCE(metrics.counter.sync_open_tab, 0)) AS sync_open_tab,
    SUM(COALESCE(metrics.counter.sync_sign_in_sync_pressed, 0)) AS sync_sign_in_sync_pressed,
    --Privacy
    SUM(COALESCE(metrics.quantity.tabs_private_tabs_quantity, 0)) AS tabs_private_tabs_quantity,
    COUNTIF(metrics.boolean.preferences_close_private_tabs) AS preferences_close_private_tabs,
    COUNTIF(metrics.boolean.tracking_protection_enabled) AS tracking_protection_enabled,
    COUNTIF(LOWER(metrics.string.tracking_protection_strength) = "strict") AS tracking_protection_strict_enabled,
    --Tab Count
    SUM(COALESCE(metrics.quantity.tabs_normal_tabs_quantity, 0)) AS tabs_normal_tabs_quantity,
    SUM(COALESCE(metrics.quantity.tabs_inactive_tabs_count, 0)) AS tabs_inactive_tabs_count,
    --Default Browser
    SUM(COALESCE(metrics.counter.app_opened_as_default_browser, 0)) AS app_opened_as_default_browser,
    SUM(COALESCE(metrics.counter.settings_menu_set_as_default_browser_pressed, 0)) AS settings_menu_set_as_default_browser_pressed,
    --Notification
    COUNTIF(metrics.boolean.preferences_sync_notifs) AS preferences_sync_notifs,
    COUNTIF(metrics.boolean.preferences_tips_and_features_notifs) AS preferences_tips_and_features_notifs,
    --Customize Home
    COUNTIF(metrics.boolean.preferences_jump_back_in) AS preferences_jump_back_in,
    COUNTIF(metrics.boolean.preferences_recently_visited) AS preferences_recently_visited,
    COUNTIF(metrics.boolean.preferences_recently_saved) AS preferences_recently_saved,
    COUNTIF(metrics.boolean.preferences_pocket) AS preferences_pocket,
    SUM(COALESCE(metrics.counter.app_menu_customize_homepage, 0)) AS app_menu_customize_homepage,
    SUM(COALESCE(metrics.counter.firefox_home_page_customize_homepage_button, 0)) AS firefox_home_page_customize_homepage_button,
  FROM firefox_ios.metrics AS metric_ping
  LEFT JOIN UNNEST(metrics.labeled_counter.bookmarks_add) AS bookmarks_add_table
  LEFT JOIN UNNEST(metrics.labeled_counter.bookmarks_delete) AS bookmarks_delete_table
  LEFT JOIN UNNEST(metrics.labeled_counter.bookmarks_edit) AS bookmarks_edit_table
  LEFT JOIN UNNEST(metrics.labeled_counter.bookmarks_open) AS bookmarks_open_table
  LEFT JOIN UNNEST(metrics.labeled_counter.bookmarks_view_list) AS bookmarks_view_list_table
  WHERE
    LOWER(metadata.isp.name) <> "browserstack"
    -- we need to work with a larger time window as some metrics ping arrive with a multi day delay
    AND DATE(submission_timestamp) BETWEEN DATE_SUB(@submission_date, INTERVAL 4 DAY) AND @submission_date
    AND DATE(DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')) = DATE_SUB(@submission_date, INTERVAL 4 DAY)
GROUP BY `date`, client_id, channel, country
)
-- Aggregated feature usage
SELECT
  `date` AS metric_date,
  channel,
  country,
  adjust_network,
  is_default_browser,
  /*Logins*/
  -- logins_deleted
  COUNT(DISTINCT IF(logins_deleted > 0, client_id, NULL)) AS logins_deleted_users,
  SUM(logins_deleted) AS logins_deleted,
  -- logins_modified
  COUNT(DISTINCT IF(logins_modified > 0, client_id, NULL)) AS logins_modified_users,
  SUM(logins_modified) AS logins_modified,
  -- logins_saved
  COUNT(DISTINCT IF(logins_saved > 0, client_id, NULL)) AS logins_saved_users,
  SUM(logins_saved) AS logins_saved,
  -- logins_saved_all
  COUNT(DISTINCT IF(logins_saved_all > 0, client_id, NULL)) AS logins_saved_all_users,
  SUM(logins_saved_all) AS logins_saved_all,
  /*Credit Card*/
  --credit card autofill enabled
  COUNT(DISTINCT IF(credit_card_autofill_enabled > 0, client_id, NULL)) AS credit_card_autofill_enabled_users,
  SUM(credit_card_autofill_enabled) AS credit_card_autofill_enabled,
  --credit_card_sync_enabled
  COUNT(DISTINCT IF(credit_card_sync_enabled > 0, client_id, NULL)) AS credit_card_sync_enabled_users,
  SUM(credit_card_sync_enabled) AS credit_card_sync_enabled,
  --credit_card_deleted
  COUNT(DISTINCT IF(credit_card_deleted > 0, client_id, NULL)) AS credit_card_deleted_users,
  SUM(credit_card_deleted) AS credit_card_deleted,
  --credit_card_modified
  COUNT(DISTINCT IF(credit_card_modified > 0, client_id, NULL)) AS credit_card_modified_users,
  SUM(credit_card_modified) AS credit_card_modified,
  --credit_card_saved
  COUNT(DISTINCT IF(credit_card_saved > 0, client_id, NULL)) AS credit_card_saved_users,
  SUM(credit_card_saved) AS credit_card_saved,
  --credit_card_saved_all
  COUNT(DISTINCT IF(credit_card_saved_all > 0, client_id, NULL)) AS credit_card_saved_all_users,
  SUM(credit_card_saved_all) AS credit_card_saved_all,
  /*Bookmark*/
  --bookmarks_add
  COUNT(DISTINCT IF(bookmarks_add > 0, client_id, NULL)) AS bookmarks_add_users,
  SUM(bookmarks_add) AS bookmarks_add,
  -- Bookmarks Delete
  COUNT(DISTINCT IF(bookmarks_delete > 0, client_id, NULL)) AS bookmarks_delete_users,
  SUM(bookmarks_delete) AS bookmarks_delete,
  -- Bookmarks Edit
  COUNT(DISTINCT IF(bookmarks_edit > 0, client_id, NULL)) AS bookmarks_edit_users,
  SUM(bookmarks_edit) AS bookmarks_edit,
  -- Has Mobile Bookmarks
  COUNT(DISTINCT IF(has_mobile_bookmarks > 0, client_id, NULL)) AS has_mobile_bookmarks_users,
  SUM(has_mobile_bookmarks) AS has_mobile_bookmarks,
  -- Mobile Bookmarks Count
  COUNT(DISTINCT IF(mobile_bookmarks_count > 0, client_id, NULL)) AS mobile_bookmarks_count_users,
  SUM(mobile_bookmarks_count) AS mobile_bookmarks_count,
  -- Bookmarks Open
  COUNT(DISTINCT IF(bookmarks_open > 0, client_id, NULL)) AS bookmarks_open_users,
  SUM(bookmarks_open) AS bookmarks_open,
  -- Bookmarks View List
  COUNT(DISTINCT IF(bookmarks_view_list > 0, client_id, NULL)) AS bookmarks_view_list_users,
  SUM(bookmarks_view_list) AS bookmarks_view_list,
  /*FxA*/
  --sync_create_account_pressed
  COUNT(DISTINCT IF(sync_create_account_pressed > 0, client_id, NULL)) AS sync_create_account_pressed_users,
  SUM(sync_create_account_pressed) AS sync_create_account_pressed,
  --sync_open_tab
  COUNT(DISTINCT IF(sync_open_tab > 0, client_id, NULL)) AS sync_open_tab_users,
  SUM(sync_open_tab) AS sync_open_tab,
  --sync_sign_in_sync_pressed
  COUNT(DISTINCT IF(sync_sign_in_sync_pressed > 0, client_id, NULL)) AS sync_sign_in_sync_pressed_users,
  SUM(sync_sign_in_sync_pressed) AS sync_sign_in_sync_pressed,
  /*Privacy*/
  --tabs_private_tabs_quantity
  COUNT(DISTINCT IF(tabs_private_tabs_quantity > 0, client_id, NULL)) AS tabs_private_tabs_quantity_users,
  SUM(tabs_private_tabs_quantity) AS tabs_private_tabs_quantity,
  -- Preferences Close Private Tabs
  COUNT(DISTINCT IF(preferences_close_private_tabs > 0, client_id, NULL)) AS preferences_close_private_tabs_users,
  SUM(preferences_close_private_tabs) AS preferences_close_private_tabs,
  -- Tracking Protection Enabled
  COUNT(DISTINCT IF(tracking_protection_enabled > 0, client_id, NULL)) AS tracking_protection_enabled_users,
  SUM(tracking_protection_enabled) AS tracking_protection_enabled,
  -- Tracking Protection Strict
  COUNT(DISTINCT IF(tracking_protection_strict_enabled > 0, client_id, NULL)) AS tracking_protection_strict_users,
  SUM(tracking_protection_strict_enabled) AS tracking_protection_strict,
  /*Tab Count*/
  --tabs_normal_tabs_quantity
  COUNT(DISTINCT IF(tabs_normal_tabs_quantity > 0, client_id, NULL)) AS tabs_normal_tabs_quantity_users,
  SUM(tabs_normal_tabs_quantity) AS tabs_normal_tabs_quantity,
  --tabs_inactive_tabs_count
  COUNT(DISTINCT IF(tabs_inactive_tabs_count > 0, client_id, NULL)) AS tabs_inactive_tabs_count_users,
  SUM(tabs_inactive_tabs_count) AS tabs_inactive_tabs_count,
  /*Default Browser*/
  --app_opened_as_default_browser
  COUNT(DISTINCT IF(app_opened_as_default_browser > 0, client_id, NULL)) AS app_opened_as_default_browser_users,
  SUM(app_opened_as_default_browser) AS app_opened_as_default_browser,
  -- settings_menu_set_as_default_browser_pressed
  COUNT(DISTINCT IF(settings_menu_set_as_default_browser_pressed > 0, client_id, NULL)) AS settings_menu_set_as_default_browser_pressed_users,
  SUM(settings_menu_set_as_default_browser_pressed) AS settings_menu_set_as_default_browser_pressed,
  /*Notification*/
  --preferences_sync_notifs
  COUNT(DISTINCT IF(preferences_sync_notifs > 0, client_id, NULL)) AS preferences_sync_notifs_users,
  SUM(preferences_sync_notifs) AS preferences_sync_notifs,
  -- preferences_tips_and_features_notifs
  COUNT(DISTINCT IF(preferences_tips_and_features_notifs > 0, client_id, NULL)) AS preferences_tips_and_features_notifs_users,
  SUM(preferences_tips_and_features_notifs) AS preferences_tips_and_features_notifs,
  /*Customize Home*/
  --preferences_jump_back_in
  COUNT(DISTINCT IF(preferences_jump_back_in > 0, client_id, NULL)) AS preferences_jump_back_in_users,
  SUM(preferences_jump_back_in) AS preferences_jump_back_in,
  -- Preferences Recently Visited
  COUNT(DISTINCT IF(preferences_recently_visited > 0, client_id, NULL)) AS preferences_recently_visited_users,
  SUM(preferences_recently_visited) AS preferences_recently_visited,
  -- Preferences Recently Saved
  COUNT(DISTINCT IF(preferences_recently_saved > 0, client_id, NULL)) AS preferences_recently_saved_users,
  SUM(preferences_recently_saved) AS preferences_recently_saved,
  -- Preferences Pocket
  COUNT(DISTINCT IF(preferences_pocket > 0, client_id, NULL)) AS preferences_pocket_users,
  SUM(preferences_pocket) AS preferences_pocket,
  -- App Menu Customize Homepage
  COUNT(DISTINCT IF(app_menu_customize_homepage > 0, client_id, NULL)) AS app_menu_customize_homepage_users,
  SUM(app_menu_customize_homepage) AS app_menu_customize_homepage,
  -- Firefox Home Page Customize Homepage Button
  COUNT(DISTINCT IF(firefox_home_page_customize_homepage_button > 0, client_id, NULL)) AS firefox_home_page_customize_homepage_button_users,
  SUM(firefox_home_page_customize_homepage_button) AS firefox_home_page_customize_homepage_button,
FROM
  metric_ping_clients_feature_usage
-- Note: baseline_clients is necessary to restrict which clients are used in this aggregation
-- to avoid situation where client count based feature usage is greater than DAU.
INNER JOIN
  baseline_clients USING(`date`, client_id, channel, country)
LEFT JOIN
  client_attribution USING(client_id, channel)
GROUP BY
  `date`,
  channel,
  country,
  adjust_network,
  is_default_browser