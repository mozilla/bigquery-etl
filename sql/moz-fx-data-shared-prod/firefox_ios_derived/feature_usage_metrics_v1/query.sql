WITH dau AS 
(SELECT DATE(LEAST(
        DATETIME(ping_info.parsed_start_time, 'UTC'),
        DATETIME(ping_info.parsed_end_time, 'UTC')
    )) AS baseline_ping_date,
client_info.client_id
FROM firefox_ios.baseline
WHERE metrics.timespan.glean_baseline_duration.value > 0
AND LOWER(COALESCE(metadata.isp.name, "")) <> "browserstack"
AND DATE(LEAST(
        DATETIME(ping_info.parsed_start_time, 'UTC'),
        DATETIME(ping_info.parsed_end_time, 'UTC')
    ))= DATE_SUB(@submission_date, INTERVAL 4 DAY)
AND DATE(submission_timestamp) BETWEEN DATE_SUB(@submission_date, INTERVAL 4 DAY) AND @submission_date
),

metrics_client AS
(SELECT DATE(LEAST(
        DATETIME(ping_info.parsed_start_time, 'UTC'),
        DATETIME(ping_info.parsed_end_time, 'UTC')
    )) AS metrics_ping_date,
client_info.client_id
FROM firefox_ios.metrics
WHERE DATE(LEAST(
        DATETIME(ping_info.parsed_start_time, 'UTC'),
        DATETIME(ping_info.parsed_end_time, 'UTC')
    ))= DATE_SUB(@submission_date, INTERVAL 4 DAY)
AND DATE(submission_timestamp) BETWEEN DATE_SUB(@submission_date, INTERVAL 4 DAY) AND @submission_date
),

metrics_dau AS
(SELECT metrics.metrics_ping_date, metrics.client_id
FROM metrics_client metrics
LEFT JOIN dau
ON metrics.metrics_ping_date = dau.baseline_ping_date
AND metrics.client_id = dau.client_id
WHERE dau.client_id IS NOT NULL
GROUP BY metrics.metrics_ping_date, metrics.client_id),


metrics_dau_pings AS
(SELECT metrics.* FROM 
(SELECT client_info.client_id,
DATE(LEAST(
        DATETIME(ping_info.parsed_start_time, 'UTC'),
        DATETIME(ping_info.parsed_end_time, 'UTC')
    )) AS metrics_ping_date,
--Credential Management: Logins
metrics.counter.logins_deleted,
metrics.counter.logins_modified,
metrics.counter.logins_saved,
metrics.quantity.logins_saved_all,
--Credential Management: Credit Cards
metrics.boolean.credit_card_autofill_enabled,
metrics.boolean.credit_card_sync_enabled,
metrics.counter.credit_card_deleted,
metrics.counter.credit_card_modified,
metrics.counter.credit_card_saved,
metrics.quantity.credit_card_saved_all,
--Bookmark
bookmarks_add_table.value AS bookmarks_add_value,
bookmarks_delete_table.value AS bookmarks_delete_value,
bookmarks_edit_table.value AS bookmarks_edit_value,
metrics.boolean.bookmarks_has_mobile_bookmarks,
metrics.quantity.bookmarks_mobile_bookmarks_count,
bookmarks_open_table.value AS bookmarks_open_value,
bookmarks_view_list_table.value AS bookmarks_view_list_value,
--FxA
metrics.counter.sync_create_account_pressed,
metrics.counter.sync_open_tab,
metrics.counter.sync_sign_in_sync_pressed,
--Privacy
metrics.quantity.tabs_private_tabs_quantity,
metrics.boolean.preferences_close_private_tabs,
metrics.boolean.tracking_protection_enabled,
metrics.string.tracking_protection_strength,
--Tab Count
metrics.quantity.tabs_normal_tabs_quantity,
metrics.quantity.tabs_inactive_tabs_count,
--Default Browser
metrics.counter.app_opened_as_default_browser,
metrics.counter.settings_menu_set_as_default_browser_pressed,
--Notification
metrics.boolean.preferences_sync_notifs,
metrics.boolean.preferences_tips_and_features_notifs,
--Customize Home
metrics.boolean.preferences_jump_back_in,
metrics.boolean.preferences_recently_visited,
metrics.boolean.preferences_recently_saved,
metrics.boolean.preferences_pocket,
metrics.counter.app_menu_customize_homepage,
metrics.counter.firefox_home_page_customize_homepage_button
FROM firefox_ios.metrics AS metric
LEFT JOIN UNNEST(metrics.labeled_counter.bookmarks_add) AS bookmarks_add_table
LEFT JOIN UNNEST(metrics.labeled_counter.bookmarks_delete) AS bookmarks_delete_table
LEFT JOIN UNNEST(metrics.labeled_counter.bookmarks_edit) AS bookmarks_edit_table
LEFT JOIN UNNEST(metrics.labeled_counter.bookmarks_open) AS bookmarks_open_table
LEFT JOIN UNNEST(metrics.labeled_counter.bookmarks_view_list) AS bookmarks_view_list_table
WHERE DATE(LEAST(
        DATETIME(ping_info.parsed_start_time, 'UTC'),
        DATETIME(ping_info.parsed_end_time, 'UTC')
    ))= DATE_SUB(@submission_date, INTERVAL 4 DAY)
AND DATE(submission_timestamp) BETWEEN DATE_SUB(@submission_date, INTERVAL 4 DAY) AND @submission_date) metrics
JOIN metrics_dau
ON DATE(metrics.metrics_ping_date) = metrics_dau.metrics_ping_date 
AND metrics.client_id = metrics_dau.client_id),

product_features AS (
  SELECT
    client_id,
    metrics_ping_date,
    /*Logins*/
    COALESCE(
      SUM(logins_deleted),
      0
    ) AS logins_deleted,
    COALESCE(
      SUM(logins_modified),
      0
    ) AS logins_modified,
    COALESCE(
      SUM(logins_saved),
      0
    ) AS logins_saved,
    COALESCE(
      SUM(logins_saved_all),
      0
    ) AS logins_saved_all,

    /*Credit Card*/
    SUM(CASE WHEN credit_card_autofill_enabled THEN 1 ELSE 0 END) AS credit_card_autofill_enabled,
    SUM(CASE WHEN credit_card_sync_enabled THEN 1 ELSE 0 END) AS credit_card_sync_enabled,
    COALESCE(SUM(credit_card_deleted), 0) AS credit_card_deleted,
    COALESCE(SUM(credit_card_modified), 0) AS credit_card_modified,
    COALESCE(SUM(credit_card_saved), 0) AS credit_card_saved,
    COALESCE(SUM(credit_card_saved_all), 0) AS credit_card_saved_all,
    /*Bookmark*/
    COALESCE(SUM(bookmarks_add_value), 0) AS bookmarks_add,
    COALESCE(
      SUM(bookmarks_delete_value),
      0
    ) AS bookmarks_delete,
    COALESCE(
      SUM(bookmarks_edit_value),
      0
    ) AS bookmarks_edit,
    SUM(CASE WHEN bookmarks_has_mobile_bookmarks THEN 1 ELSE 0 END) AS has_mobile_bookmarks,
    COALESCE(SUM(bookmarks_mobile_bookmarks_count), 0) AS mobile_bookmarks_count,
    COALESCE(
      SUM(bookmarks_open_value),
      0
    ) AS bookmarks_open,
    COALESCE(
      SUM(bookmarks_view_list_value),
      0
    ) AS bookmarks_view_list,
    /*FxA*/
    COALESCE(SUM(sync_create_account_pressed), 0) AS sync_create_account_pressed,
    COALESCE(SUM(sync_open_tab), 0) AS sync_open_tab,
    COALESCE(SUM(sync_sign_in_sync_pressed), 0) AS sync_sign_in_sync_pressed,
    /*Privacy*/
    COALESCE(SUM(tabs_private_tabs_quantity), 0) AS tabs_private_tabs_quantity,
    SUM(CASE WHEN preferences_close_private_tabs THEN 1 ELSE 0 END) AS preferences_close_private_tabs,
    SUM(CASE WHEN tracking_protection_enabled THEN 1 ELSE 0 END) AS tracking_protection_enabled,
    SUM(CASE WHEN tracking_protection_strength = 'strict' THEN 1 ELSE 0 END) AS tracking_protection_strict,
    /*Tab Count*/
    COALESCE(SUM(tabs_normal_tabs_quantity), 0) AS tabs_normal_tabs_quantity,
    COALESCE(SUM(tabs_inactive_tabs_count), 0) AS tabs_inactive_tabs_count,
    /*Default Browser*/
    COALESCE(
      SUM(app_opened_as_default_browser),
      0
    ) AS app_opened_as_default_browser,
    COALESCE(
      SUM(settings_menu_set_as_default_browser_pressed),
      0
    ) AS settings_menu_set_as_default_browser_pressed,
    /*Notification*/
    SUM(CASE WHEN preferences_sync_notifs THEN 1 ELSE 0 END) AS preferences_sync_notifs,
    SUM(CASE WHEN preferences_tips_and_features_notifs THEN 1 ELSE 0 END) AS preferences_tips_and_features_notifs,
    /*Customize Home*/
    SUM(CASE WHEN preferences_jump_back_in THEN 1 ELSE 0 END) AS preferences_jump_back_in,
    SUM(CASE WHEN preferences_recently_visited THEN 1 ELSE 0 END) AS preferences_recently_visited,
    SUM(CASE WHEN preferences_recently_saved THEN 1 ELSE 0 END) AS preferences_recently_saved,
    SUM(CASE WHEN preferences_pocket THEN 1 ELSE 0 END) AS preferences_pocket,
    COALESCE(SUM(app_menu_customize_homepage), 0) AS app_menu_customize_homepage,
    COALESCE(
      SUM(firefox_home_page_customize_homepage_button),
      0
    ) AS firefox_home_page_customize_homepage_button
  FROM
    metrics_dau_pings AS metric
  WHERE
    metrics_ping_date = DATE_SUB(@submission_date, INTERVAL 4 DAY)
  GROUP BY
    client_id,
    metrics_ping_date
),

product_features_agg AS (
  SELECT
    metrics_ping_date,
    /*Logins*/
    --logins_deleted
    COUNT(
      DISTINCT
      CASE
        WHEN logins_deleted > 0
          THEN client_id
      END
    ) AS logins_deleted_users,
    SUM(logins_deleted) AS logins_deleted,
    --logins_modified
    COUNT(
      DISTINCT
      CASE
        WHEN logins_modified > 0
          THEN client_id
      END
    ) AS logins_modified_users,
    SUM(logins_modified) AS logins_modified,
    --logins_saved
    COUNT(
      DISTINCT
      CASE
        WHEN logins_saved > 0
          THEN client_id
      END
    ) AS logins_saved_users,
    SUM(logins_saved) AS logins_saved,
    --logins_saved_all
    COUNT(
      DISTINCT
      CASE
        WHEN logins_saved_all > 0
          THEN client_id
      END
    ) AS logins_saved_all_users,
    SUM(logins_saved_all) AS logins_saved_all,
    /*Credit Card*/
    --credit card autofill enabled
    COUNT(
      DISTINCT
      CASE
        WHEN credit_card_autofill_enabled > 0
          THEN client_id
      END
    ) AS credit_card_autofill_enabled_users,
    SUM(credit_card_autofill_enabled) AS credit_card_autofill_enabled,
    --credit_card_sync_enabled
    COUNT(
      DISTINCT
      CASE
        WHEN credit_card_sync_enabled > 0
          THEN client_id
      END
    ) AS credit_card_sync_enabled_users,
    SUM(credit_card_sync_enabled) AS credit_card_sync_enabled,
    --credit_card_deleted
    COUNT(
      DISTINCT
      CASE
        WHEN credit_card_deleted > 0
          THEN client_id
      END
    ) AS credit_card_deleted_users,
    SUM(credit_card_deleted) AS credit_card_deleted,
    --credit_card_modified
    COUNT(
      DISTINCT
      CASE
        WHEN credit_card_modified > 0
          THEN client_id
      END
    ) AS credit_card_modified_users,
    SUM(credit_card_modified) AS credit_card_modified,
    --credit_card_saved
    COUNT(
      DISTINCT
      CASE
        WHEN credit_card_saved > 0
          THEN client_id
      END
    ) AS credit_card_saved_users,
    SUM(credit_card_saved) AS credit_card_saved,
    --credit_card_saved_all
    COUNT(
      DISTINCT
      CASE
        WHEN credit_card_saved_all > 0
          THEN client_id
      END
    ) AS credit_card_saved_all_users,
    SUM(credit_card_saved_all) AS credit_card_saved_all,
    /*Bookmark*/
    --bookmarks_add
    COUNT(DISTINCT CASE WHEN bookmarks_add > 0 THEN client_id END) AS bookmarks_add_users,
    SUM(bookmarks_add) AS bookmarks_add,
    -- Bookmarks Delete
    COUNT(DISTINCT CASE WHEN bookmarks_delete > 0 THEN client_id END) AS bookmarks_delete_users,
    SUM(bookmarks_delete) AS bookmarks_delete,
    -- Bookmarks Edit
    COUNT(DISTINCT CASE WHEN bookmarks_edit > 0 THEN client_id END) AS bookmarks_edit_users,
    SUM(bookmarks_edit) AS bookmarks_edit,
    -- Has Mobile Bookmarks
    COUNT(
      DISTINCT
      CASE
        WHEN has_mobile_bookmarks > 0
          THEN client_id
      END
    ) AS has_mobile_bookmarks_users,
    SUM(has_mobile_bookmarks) AS has_mobile_bookmarks,
    -- Mobile Bookmarks Count
    COUNT(
      DISTINCT
      CASE
        WHEN mobile_bookmarks_count > 0
          THEN client_id
      END
    ) AS mobile_bookmarks_count_users,
    SUM(mobile_bookmarks_count) AS mobile_bookmarks_count,
    -- Bookmarks Open
    COUNT(DISTINCT CASE WHEN bookmarks_open > 0 THEN client_id END) AS bookmarks_open_users,
    SUM(bookmarks_open) AS bookmarks_open,
    -- Bookmarks View List
    COUNT(
      DISTINCT
      CASE
        WHEN bookmarks_view_list > 0
          THEN client_id
      END
    ) AS bookmarks_view_list_users,
    SUM(bookmarks_view_list) AS bookmarks_view_list,
    /*FxA*/
    --sync_create_account_pressed
    COUNT(
      DISTINCT
      CASE
        WHEN sync_create_account_pressed > 0
          THEN client_id
      END
    ) AS sync_create_account_pressed_users,
    SUM(sync_create_account_pressed) AS sync_create_account_pressed,
    --sync_open_tab
    COUNT(DISTINCT CASE WHEN sync_open_tab > 0 THEN client_id END) AS sync_open_tab_users,
    SUM(sync_open_tab) AS sync_open_tab,
    --sync_sign_in_sync_pressed
    COUNT(
      DISTINCT
      CASE
        WHEN sync_sign_in_sync_pressed > 0
          THEN client_id
      END
    ) AS sync_sign_in_sync_pressed_users,
    SUM(sync_sign_in_sync_pressed) AS sync_sign_in_sync_pressed,
    /*Privacy*/
    --tabs_private_tabs_quantity
    COUNT(
      DISTINCT
      CASE
        WHEN tabs_private_tabs_quantity > 0
          THEN client_id
      END
    ) AS tabs_private_tabs_quantity_users,
    SUM(tabs_private_tabs_quantity) AS tabs_private_tabs_quantity,
    -- Preferences Close Private Tabs
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_close_private_tabs > 0
          THEN client_id
      END
    ) AS preferences_close_private_tabs_users,
    SUM(preferences_close_private_tabs) AS preferences_close_private_tabs,
    -- Tracking Protection Enabled
    COUNT(
      DISTINCT
      CASE
        WHEN tracking_protection_enabled > 0
          THEN client_id
      END
    ) AS tracking_protection_enabled_users,
    SUM(tracking_protection_enabled) AS tracking_protection_enabled,
    -- Tracking Protection Strict
    COUNT(
      DISTINCT
      CASE
        WHEN tracking_protection_strict > 0
          THEN client_id
      END
    ) AS tracking_protection_strict_users,
    SUM(tracking_protection_strict) AS tracking_protection_strict,
    /*Tab Count*/
    --tabs_normal_tabs_quantity
    COUNT(
      DISTINCT
      CASE
        WHEN tabs_normal_tabs_quantity > 0
          THEN client_id
      END
    ) AS tabs_normal_tabs_quantity_users,
    SUM(tabs_normal_tabs_quantity) AS tabs_normal_tabs_quantity,
    --tabs_inactive_tabs_count
    COUNT(
      DISTINCT
      CASE
        WHEN tabs_inactive_tabs_count > 0
          THEN client_id
      END
    ) AS tabs_inactive_tabs_count_users,
    SUM(tabs_inactive_tabs_count) AS tabs_inactive_tabs_count,
    /*Default Browser*/
    --app_opened_as_default_browser
    COUNT(
      DISTINCT
      CASE
        WHEN app_opened_as_default_browser > 0
          THEN client_id
      END
    ) AS app_opened_as_default_browser_users,
    SUM(app_opened_as_default_browser) AS app_opened_as_default_browser,
    -- settings_menu_set_as_default_browser_pressed
    COUNT(
      DISTINCT
      CASE
        WHEN settings_menu_set_as_default_browser_pressed > 0
          THEN client_id
      END
    ) AS settings_menu_set_as_default_browser_pressed_users,
    SUM(settings_menu_set_as_default_browser_pressed) AS settings_menu_set_as_default_browser_pressed,
    /*Notification*/
    --preferences_sync_notifs
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_sync_notifs > 0
          THEN client_id
      END
    ) AS preferences_sync_notifs_users,
    SUM(preferences_sync_notifs) AS preferences_sync_notifs,
    -- preferences_tips_and_features_notifs
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_tips_and_features_notifs > 0
          THEN client_id
      END
    ) AS preferences_tips_and_features_notifs_users,
    SUM(preferences_tips_and_features_notifs) AS preferences_tips_and_features_notifs,
    /*Customize Home*/
    --preferences_jump_back_in
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_jump_back_in > 0
          THEN client_id
      END
    ) AS preferences_jump_back_in_users,
    SUM(preferences_jump_back_in) AS preferences_jump_back_in,
    -- Preferences Recently Visited
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_recently_visited > 0
          THEN client_id
      END
    ) AS preferences_recently_visited_users,
    SUM(preferences_recently_visited) AS preferences_recently_visited,
    -- Preferences Recently Saved
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_recently_saved > 0
          THEN client_id
      END
    ) AS preferences_recently_saved_users,
    SUM(preferences_recently_saved) AS preferences_recently_saved,
    -- Preferences Pocket
    COUNT(DISTINCT CASE WHEN preferences_pocket > 0 THEN client_id END) AS preferences_pocket_users,
    SUM(preferences_pocket) AS preferences_pocket,
    -- App Menu Customize Homepage
    COUNT(
      DISTINCT
      CASE
        WHEN app_menu_customize_homepage > 0
          THEN client_id
      END
    ) AS app_menu_customize_homepage_users,
    SUM(app_menu_customize_homepage) AS app_menu_customize_homepage,
    -- Firefox Home Page Customize Homepage Button
    COUNT(
      DISTINCT
      CASE
        WHEN firefox_home_page_customize_homepage_button > 0
          THEN client_id
      END
    ) AS firefox_home_page_customize_homepage_button_users,
    SUM(firefox_home_page_customize_homepage_button) AS firefox_home_page_customize_homepage_button
  FROM
    product_features
  GROUP BY
    metrics_ping_date
)
SELECT
  metrics_ping_date,
/*Logins*/
  logins_deleted_users,
  logins_deleted,
  logins_modified_users,
  logins_modified,
  logins_saved_users,
  logins_saved,
  logins_saved_all_users,
  logins_saved_all,
/*Credit Card*/
  credit_card_autofill_enabled_users,
  credit_card_autofill_enabled,
  credit_card_sync_enabled_users,
  credit_card_sync_enabled,
/*Bookmark*/
  bookmarks_add_users,
  bookmarks_add,
  bookmarks_delete_users,
  bookmarks_delete,
  bookmarks_edit_users,
  bookmarks_edit,
  has_mobile_bookmarks_users,
  has_mobile_bookmarks,
  mobile_bookmarks_count_users,
  mobile_bookmarks_count,
  bookmarks_open_users,
  bookmarks_open,
  bookmarks_view_list_users,
  bookmarks_view_list,
/*FxA*/
  sync_create_account_pressed_users,
  sync_create_account_pressed,
  sync_open_tab_users,
  sync_open_tab,
  sync_sign_in_sync_pressed_users,
  sync_sign_in_sync_pressed,
/*Privacy*/
  tabs_private_tabs_quantity_users,
  tabs_private_tabs_quantity,
  preferences_close_private_tabs_users,
  preferences_close_private_tabs,
  tracking_protection_enabled_users,
  tracking_protection_enabled,
  tracking_protection_strict_users,
  tracking_protection_strict,
/*Tab Count*/
  tabs_normal_tabs_quantity_users,
  tabs_normal_tabs_quantity,
  tabs_inactive_tabs_count_users,
  tabs_inactive_tabs_count,
/*Default Browser*/
  app_opened_as_default_browser_users,
  app_opened_as_default_browser,
  settings_menu_set_as_default_browser_pressed_users,
  settings_menu_set_as_default_browser_pressed,
/*Notification*/
  preferences_sync_notifs_users,
  preferences_sync_notifs,
  preferences_tips_and_features_notifs_users,
  preferences_tips_and_features_notifs,
/*Customize Home*/
  preferences_jump_back_in_users,
  preferences_jump_back_in,
  preferences_recently_visited_users,
  preferences_recently_visited,
  preferences_recently_saved_users,
  preferences_recently_saved,
  preferences_pocket_users,
  preferences_pocket,
  app_menu_customize_homepage_users,
  app_menu_customize_homepage,
  firefox_home_page_customize_homepage_button_users,
  firefox_home_page_customize_homepage_button,
/*new credit card probes*/
  credit_card_deleted_users,
  credit_card_deleted,
  credit_card_modified_users,
  credit_card_modified,
  credit_card_saved_users,
  credit_card_saved,
  credit_card_saved_all_users
  credit_card_saved_all
FROM product_features_agg

