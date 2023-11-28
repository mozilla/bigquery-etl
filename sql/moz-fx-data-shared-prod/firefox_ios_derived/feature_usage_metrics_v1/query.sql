-- Query for firefox_ios_derived.feature_usage_metrics_v1
-- For more information on writing queries see:
-- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
DECLARE start_date DATE DEFAULT "2021-01-01";

DECLARE end_date DATE DEFAULT current_date;

WITH dau_segments AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    COUNT(DISTINCT client_info.client_id) AS dau
  FROM
    `firefox_ios.metrics`
  WHERE
    DATE(submission_timestamp) >= start_date
  GROUP BY
    submission_date
),
product_features AS (
  SELECT
    client_info.client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    /*Credit Card*/
    COALESCE(
      SUM(CASE WHEN metrics.boolean.credit_card_autofill_enabled THEN 1 ELSE 0 END),
      0
    ) AS credit_card_autofill_enabled,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.credit_card_sync_enabled THEN 1 ELSE 0 END),
      0
    ) AS credit_card_sync_enabled,
    /*Bookmark*/
    COALESCE(SUM(metrics.labeled_counter.bookmarks_add[SAFE_OFFSET(0)].value), 0) AS bookmarks_add,
    COALESCE(
      SUM(metrics.labeled_counter.bookmarks_delete[SAFE_OFFSET(0)].value),
      0
    ) AS bookmarks_delete,
    COALESCE(
      SUM(metrics.labeled_counter.bookmarks_edit[SAFE_OFFSET(0)].value),
      0
    ) AS bookmarks_edit,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.bookmarks_has_mobile_bookmarks THEN 1 ELSE 0 END),
      0
    ) AS has_mobile_bookmarks,
    COALESCE(SUM(metrics.quantity.bookmarks_mobile_bookmarks_count), 0) AS mobile_bookmarks_count,
    COALESCE(
      SUM(metrics.labeled_counter.bookmarks_open[SAFE_OFFSET(0)].value),
      0
    ) AS bookmarks_open,
    COALESCE(
      SUM(metrics.labeled_counter.bookmarks_view_list[SAFE_OFFSET(0)].value),
      0
    ) AS bookmarks_view_list,
    /*FxA*/
    COALESCE(SUM(metrics.counter.sync_create_account_pressed), 0) AS sync_create_account_pressed,
    COALESCE(SUM(metrics.counter.sync_open_tab), 0) AS sync_open_tab,
    COALESCE(SUM(metrics.counter.sync_sign_in_sync_pressed), 0) AS sync_sign_in_sync_pressed,
    /*Privacy*/
    COALESCE(SUM(metrics.quantity.tabs_private_tabs_quantity), 0) AS tabs_private_tabs_quantity,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.preferences_close_private_tabs THEN 1 ELSE 0 END),
      0
    ) AS preferences_close_private_tabs,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.tracking_protection_enabled THEN 1 ELSE 0 END),
      0
    ) AS tracking_protection_enabled,
    COALESCE(
      SUM(CASE WHEN metrics.string.tracking_protection_strength = 'strict' THEN 1 ELSE 0 END),
      0
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
    COALESCE(
      SUM(CASE WHEN metrics.boolean.preferences_sync_notifs THEN 1 ELSE 0 END),
      0
    ) AS preferences_sync_notifs,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.preferences_tips_and_features_notifs THEN 1 ELSE 0 END),
      0
    ) AS preferences_tips_and_features_notifs,
    /*Customize Home*/
    COALESCE(
      SUM(CASE WHEN metrics.boolean.preferences_jump_back_in THEN 1 ELSE 0 END),
      0
    ) AS preferences_jump_back_in,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.preferences_recently_visited THEN 1 ELSE 0 END),
      0
    ) AS preferences_recently_visited,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.preferences_recently_saved THEN 1 ELSE 0 END),
      0
    ) AS preferences_recently_saved,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.preferences_pocket THEN 1 ELSE 0 END),
      0
    ) AS preferences_pocket,
    COALESCE(SUM(metrics.counter.app_menu_customize_homepage), 0) AS app_menu_customize_homepage,
    COALESCE(
      SUM(metrics.counter.firefox_home_page_customize_homepage_button),
      0
    ) AS firefox_home_page_customize_homepage_button
  FROM
    `firefox_ios.metrics`
  WHERE
    DATE(submission_timestamp) >= start_date
  GROUP BY
    client_id,
    submission_date
),
product_features_agg AS (
  SELECT
    submission_date,
    /*Credit Card*/
    --credit card autofill enabled
    COUNT(
      DISTINCT
      CASE
        WHEN credit_card_autofill_enabled > 0
          THEN client_id
      END
    ) AS credit_card_autofill_enabled_users,
    COALESCE(SUM(credit_card_autofill_enabled), 0) AS credit_card_autofill_enabled,
    --credit_card_sync_enabled
    COUNT(
      DISTINCT
      CASE
        WHEN credit_card_sync_enabled > 0
          THEN client_id
      END
    ) AS credit_card_sync_enabled_users,
    COALESCE(SUM(credit_card_sync_enabled), 0) AS credit_card_sync_enabled,
    /*Bookmark*/
    --bookmarks_add
    COUNT(DISTINCT CASE WHEN bookmarks_add > 0 THEN client_id END) AS bookmarks_add_users,
    COALESCE(SUM(bookmarks_add), 0) AS bookmarks_add,
    -- Bookmarks Delete
    COUNT(DISTINCT CASE WHEN bookmarks_delete > 0 THEN client_id END) AS bookmarks_delete_users,
    COALESCE(SUM(bookmarks_delete), 0) AS bookmarks_delete,
    -- Bookmarks Edit
    COUNT(DISTINCT CASE WHEN bookmarks_edit > 0 THEN client_id END) AS bookmarks_edit_users,
    COALESCE(SUM(bookmarks_edit), 0) AS bookmarks_edit,
    -- Has Mobile Bookmarks
    COUNT(
      DISTINCT
      CASE
        WHEN has_mobile_bookmarks > 0
          THEN client_id
      END
    ) AS has_mobile_bookmarks_users,
    COALESCE(SUM(has_mobile_bookmarks), 0) AS has_mobile_bookmarks,
    -- Mobile Bookmarks Count
    COUNT(
      DISTINCT
      CASE
        WHEN mobile_bookmarks_count > 0
          THEN client_id
      END
    ) AS mobile_bookmarks_count_users,
    COALESCE(SUM(mobile_bookmarks_count), 0) AS mobile_bookmarks_count,
    -- Bookmarks Open
    COUNT(DISTINCT CASE WHEN bookmarks_open > 0 THEN client_id END) AS bookmarks_open_users,
    COALESCE(SUM(bookmarks_open), 0) AS bookmarks_open,
    -- Bookmarks View List
    COUNT(
      DISTINCT
      CASE
        WHEN bookmarks_view_list > 0
          THEN client_id
      END
    ) AS bookmarks_view_list_users,
    COALESCE(SUM(bookmarks_view_list), 0) AS bookmarks_view_list,
    /*FxA*/
    --sync_create_account_pressed
    COUNT(
      DISTINCT
      CASE
        WHEN sync_create_account_pressed > 0
          THEN client_id
      END
    ) AS sync_create_account_pressed_users,
    COALESCE(SUM(sync_create_account_pressed), 0) AS sync_create_account_pressed,
    --sync_open_tab
    COUNT(DISTINCT CASE WHEN sync_open_tab > 0 THEN client_id END) AS sync_open_tab_users,
    COALESCE(SUM(sync_open_tab), 0) AS sync_open_tab,
    --sync_sign_in_sync_pressed
    COUNT(
      DISTINCT
      CASE
        WHEN sync_sign_in_sync_pressed > 0
          THEN client_id
      END
    ) AS sync_sign_in_sync_pressed_users,
    COALESCE(SUM(sync_sign_in_sync_pressed), 0) AS sync_sign_in_sync_pressed,
    /*Privacy*/
    --tabs_private_tabs_quantity
    COUNT(
      DISTINCT
      CASE
        WHEN tabs_private_tabs_quantity > 0
          THEN client_id
      END
    ) AS tabs_private_tabs_quantity_users,
    COALESCE(SUM(tabs_private_tabs_quantity), 0) AS tabs_private_tabs_quantity,
    -- Preferences Close Private Tabs
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_close_private_tabs > 0
          THEN client_id
      END
    ) AS preferences_close_private_tabs_users,
    COALESCE(SUM(preferences_close_private_tabs), 0) AS preferences_close_private_tabs,
    -- Tracking Protection Enabled
    COUNT(
      DISTINCT
      CASE
        WHEN tracking_protection_enabled > 0
          THEN client_id
      END
    ) AS tracking_protection_enabled_users,
    COALESCE(SUM(tracking_protection_enabled), 0) AS tracking_protection_enabled,
    -- Tracking Protection Strict
    COUNT(
      DISTINCT
      CASE
        WHEN tracking_protection_strict > 0
          THEN client_id
      END
    ) AS tracking_protection_strict_users,
    COALESCE(SUM(tracking_protection_strict), 0) AS tracking_protection_strict,
    /*Tab Count*/
    --tabs_normal_tabs_quantity
    COUNT(
      DISTINCT
      CASE
        WHEN tabs_normal_tabs_quantity > 0
          THEN client_id
      END
    ) AS tabs_normal_tabs_quantity_users,
    COALESCE(SUM(tabs_normal_tabs_quantity), 0) AS tabs_normal_tabs_quantity,
    --tabs_inactive_tabs_count
    COUNT(
      DISTINCT
      CASE
        WHEN tabs_inactive_tabs_count > 0
          THEN client_id
      END
    ) AS tabs_inactive_tabs_count_users,
    COALESCE(SUM(tabs_inactive_tabs_count), 0) AS tabs_inactive_tabs_count,
    /*Default Browser*/
    --app_opened_as_default_browser
    COUNT(
      DISTINCT
      CASE
        WHEN app_opened_as_default_browser > 0
          THEN client_id
      END
    ) AS app_opened_as_default_browser_users,
    COALESCE(SUM(app_opened_as_default_browser), 0) AS app_opened_as_default_browser,
    -- settings_menu_set_as_default_browser_pressed
    COUNT(
      DISTINCT
      CASE
        WHEN settings_menu_set_as_default_browser_pressed > 0
          THEN client_id
      END
    ) AS settings_menu_set_as_default_browser_pressed_users,
    COALESCE(
      SUM(settings_menu_set_as_default_browser_pressed),
      0
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
    COALESCE(SUM(preferences_sync_notifs), 0) AS preferences_sync_notifs,
    -- preferences_tips_and_features_notifs
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_tips_and_features_notifs > 0
          THEN client_id
      END
    ) AS preferences_tips_and_features_notifs_users,
    COALESCE(SUM(preferences_tips_and_features_notifs), 0) AS preferences_tips_and_features_notifs,
    /*Customize Home*/
    --preferences_jump_back_in
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_jump_back_in > 0
          THEN client_id
      END
    ) AS preferences_jump_back_in_users,
    COALESCE(SUM(preferences_jump_back_in), 0) AS preferences_jump_back_in,
    -- Preferences Recently Visited
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_recently_visited > 0
          THEN client_id
      END
    ) AS preferences_recently_visited_users,
    COALESCE(SUM(preferences_recently_visited), 0) AS preferences_recently_visited,
    -- Preferences Recently Saved
    COUNT(
      DISTINCT
      CASE
        WHEN preferences_recently_saved > 0
          THEN client_id
      END
    ) AS preferences_recently_saved_users,
    COALESCE(SUM(preferences_recently_saved), 0) AS preferences_recently_saved,
    -- Preferences Pocket
    COUNT(DISTINCT CASE WHEN preferences_pocket > 0 THEN client_id END) AS preferences_pocket_users,
    COALESCE(SUM(preferences_pocket), 0) AS preferences_pocket,
    -- App Menu Customize Homepage
    COUNT(
      DISTINCT
      CASE
        WHEN app_menu_customize_homepage > 0
          THEN client_id
      END
    ) AS app_menu_customize_homepage_users,
    COALESCE(SUM(app_menu_customize_homepage), 0) AS app_menu_customize_homepage,
    -- Firefox Home Page Customize Homepage Button
    COUNT(
      DISTINCT
      CASE
        WHEN firefox_home_page_customize_homepage_button > 0
          THEN client_id
      END
    ) AS firefox_home_page_customize_homepage_button_users,
    COALESCE(
      SUM(firefox_home_page_customize_homepage_button),
      0
    ) AS firefox_home_page_customize_homepage_button
  FROM
    product_features
  WHERE
    submission_date >= start_date
  GROUP BY
    submission_date
)
SELECT
  submission_date,
  dau,
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
  dau_segments
JOIN
  product_features_agg
USING
  (submission_date)
