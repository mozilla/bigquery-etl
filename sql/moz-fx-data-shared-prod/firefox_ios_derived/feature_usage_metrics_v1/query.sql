WITH _metrics_ping_distinct_client_count AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    COUNT(DISTINCT client_info.client_id) AS metrics_ping_distinct_client_count
  FROM
    firefox_ios.metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date
),
product_features AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    /*Logins*/
    COALESCE(SUM(metrics.counter.logins_deleted), 0) AS logins_deleted,
    COALESCE(SUM(metrics.counter.logins_modified), 0) AS logins_modified,
    COALESCE(SUM(metrics.counter.logins_saved), 0) AS logins_saved,
    COALESCE(SUM(metrics.quantity.logins_saved_all), 0) AS logins_saved_all,
    /*Credit Card*/
    SUM(
      CASE
        WHEN metrics.boolean.credit_card_autofill_enabled
          THEN 1
        ELSE 0
      END
    ) AS credit_card_autofill_enabled,
    SUM(
      CASE
        WHEN metrics.boolean.credit_card_sync_enabled
          THEN 1
        ELSE 0
      END
    ) AS credit_card_sync_enabled,
    /*Bookmark*/
    COALESCE(SUM(bookmarks_add_table.value), 0) AS bookmarks_add,
    COALESCE(SUM(bookmarks_delete_table.value), 0) AS bookmarks_delete,
    COALESCE(SUM(bookmarks_edit_table.value), 0) AS bookmarks_edit,
    SUM(
      CASE
        WHEN metrics.boolean.bookmarks_has_mobile_bookmarks
          THEN 1
        ELSE 0
      END
    ) AS has_mobile_bookmarks,
    COALESCE(SUM(metrics.quantity.bookmarks_mobile_bookmarks_count), 0) AS mobile_bookmarks_count,
    COALESCE(SUM(bookmarks_open_table.value), 0) AS bookmarks_open,
    COALESCE(SUM(bookmarks_view_list_table.value), 0) AS bookmarks_view_list,
    /*FxA*/
    COALESCE(SUM(metrics.counter.sync_create_account_pressed), 0) AS sync_create_account_pressed,
    COALESCE(SUM(metrics.counter.sync_open_tab), 0) AS sync_open_tab,
    COALESCE(SUM(metrics.counter.sync_sign_in_sync_pressed), 0) AS sync_sign_in_sync_pressed,
    /*Privacy*/
    COALESCE(SUM(metrics.quantity.tabs_private_tabs_quantity), 0) AS tabs_private_tabs_quantity,
    SUM(
      CASE
        WHEN metrics.boolean.preferences_close_private_tabs
          THEN 1
        ELSE 0
      END
    ) AS preferences_close_private_tabs,
    SUM(
      CASE
        WHEN metrics.boolean.tracking_protection_enabled
          THEN 1
        ELSE 0
      END
    ) AS tracking_protection_enabled,
    SUM(
      CASE
        WHEN metrics.string.tracking_protection_strength = 'strict'
          THEN 1
        ELSE 0
      END
    ) AS tracking_protection_strict,
    /*Tab Count*/
    COALESCE(SUM(metrics.quantity.tabs_normal_tabs_quantity), 0) AS tabs_normal_tabs_quantity,
    COALESCE(SUM(metrics.quantity.tabs_inactive_tabs_count), 0) AS tabs_inactive_tabs_count,
    /*Default Browser*/
    COALESCE(
      SUM(metrics.counter.app_opened_as_default_browser),
      0
    ) AS app_opened_as_default_browser,
    COALESCE(
      SUM(metrics.counter.settings_menu_set_as_default_browser_pressed),
      0
    ) AS settings_menu_set_as_default_browser_pressed,
    /*Notification*/
    SUM(
      CASE
        WHEN metrics.boolean.preferences_sync_notifs
          THEN 1
        ELSE 0
      END
    ) AS preferences_sync_notifs,
    SUM(
      CASE
        WHEN metrics.boolean.preferences_tips_and_features_notifs
          THEN 1
        ELSE 0
      END
    ) AS preferences_tips_and_features_notifs,
    /*Customize Home*/
    SUM(
      CASE
        WHEN metrics.boolean.preferences_jump_back_in
          THEN 1
        ELSE 0
      END
    ) AS preferences_jump_back_in,
    SUM(
      CASE
        WHEN metrics.boolean.preferences_recently_visited
          THEN 1
        ELSE 0
      END
    ) AS preferences_recently_visited,
    SUM(
      CASE
        WHEN metrics.boolean.preferences_recently_saved
          THEN 1
        ELSE 0
      END
    ) AS preferences_recently_saved,
    SUM(CASE WHEN metrics.boolean.preferences_pocket THEN 1 ELSE 0 END) AS preferences_pocket,
    COALESCE(SUM(metrics.counter.app_menu_customize_homepage), 0) AS app_menu_customize_homepage,
    COALESCE(
      SUM(metrics.counter.firefox_home_page_customize_homepage_button),
      0
    ) AS firefox_home_page_customize_homepage_button
  FROM
    firefox_ios.metrics AS metric
  LEFT JOIN
    UNNEST(metric.metrics.labeled_counter.bookmarks_add) AS bookmarks_add_table
  LEFT JOIN
    UNNEST(metric.metrics.labeled_counter.bookmarks_delete) AS bookmarks_delete_table
  LEFT JOIN
    UNNEST(metric.metrics.labeled_counter.bookmarks_edit) AS bookmarks_edit_table
  LEFT JOIN
    UNNEST(metric.metrics.labeled_counter.bookmarks_open) AS bookmarks_open_table
  LEFT JOIN
    UNNEST(metric.metrics.labeled_counter.bookmarks_view_list) AS bookmarks_view_list_table
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id,
    submission_date
),
product_features_agg AS (
  SELECT
    submission_date,
    /*Logins*/
    --logins_deleted
    COUNT(DISTINCT CASE WHEN logins_deleted > 0 THEN client_id END) AS logins_deleted_users,
    SUM(logins_deleted) AS logins_deleted,
    --logins_modified
    COUNT(DISTINCT CASE WHEN logins_modified > 0 THEN client_id END) AS logins_modified_users,
    SUM(logins_modified) AS logins_modified,
    --logins_saved
    COUNT(DISTINCT CASE WHEN logins_saved > 0 THEN client_id END) AS logins_saved_users,
    SUM(logins_saved) AS logins_saved,
    --logins_saved_all
    COUNT(DISTINCT CASE WHEN logins_saved_all > 0 THEN client_id END) AS logins_saved_all_users,
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
    SUM(
      settings_menu_set_as_default_browser_pressed
    ) AS settings_menu_set_as_default_browser_pressed,
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
    submission_date
)
SELECT
  submission_date,
  metrics_ping_distinct_client_count,
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
  firefox_home_page_customize_homepage_button
FROM
  _metrics_ping_distinct_client_count
JOIN
  product_features_agg
USING
  (submission_date)
