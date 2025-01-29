WITH baseline_clients AS (
  SELECT
    submission_date AS dau_date,
    client_id,
    LEAD(submission_date) OVER (PARTITION BY client_id ORDER BY submission_date) AS next_dau
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline_clients_daily`
  WHERE
    submission_date >= DATE_SUB(@submission_date, INTERVAL 4 DAY)
    AND durations > 0
    AND LOWER(COALESCE(isp, "")) <> "browserstack"
),
client_attribution AS (
  SELECT
    client_id,
    adjust_network,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.attribution_clients`
),
metrics_dau AS (
  -- assign a DAU date for each metric ping while keeping it de-duplicated
  SELECT
    m.*,
    MAX(dau_date) OVER (PARTITION BY document_id) AS dau_date
  FROM
    `moz-fx-data-shared-prod.firefox_ios.metrics` m
  JOIN
    baseline_clients
    ON client_info.client_id = client_id
    -- offset by at least one to reflect metrics ping design considerations
    AND DATE_DIFF(DATE(submission_timestamp), dau_date, DAY)
    BETWEEN 1
    AND 4
    -- exclude metrics pings that should be matched to next DAU date
    AND DATE(submission_timestamp) <= DATE_ADD(next_dau, INTERVAL 1 DAY)
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 3 DAY)
    AND @submission_date
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) = 1
),
metric_ping_clients_feature_usage AS (
  SELECT
    dau_date,
    client_info.client_id,
    ARRAY_AGG(normalized_channel ORDER BY submission_timestamp DESC)[SAFE_OFFSET(0)] AS channel,
    ARRAY_AGG(normalized_country_code ORDER BY submission_timestamp DESC)[
      SAFE_OFFSET(0)
    ] AS country,
    IF(SUM(metrics.counter.app_opened_as_default_browser) > 0, TRUE, FALSE) AS is_default_browser,
    --Credential Management: Logins
    SUM(COALESCE(metrics.counter.logins_deleted, 0)) AS logins_deleted,
    SUM(COALESCE(metrics.counter.logins_modified, 0)) AS logins_modified,
    SUM(COALESCE(metrics.counter.logins_saved, 0)) AS logins_saved,
    MAX(COALESCE(metrics.quantity.logins_saved_all, 0)) AS logins_saved_all,
    --Credential Management: Credit Cards
    CAST(
      MAX(COALESCE(metrics.boolean.credit_card_autofill_enabled, FALSE)) AS INT64
    ) AS credit_card_autofill_enabled,
    CAST(
      MAX(COALESCE(metrics.boolean.credit_card_sync_enabled, FALSE)) AS INT64
    ) AS credit_card_sync_enabled,
    SUM(COALESCE(metrics.counter.credit_card_deleted, 0)) AS credit_card_deleted,
    SUM(COALESCE(metrics.counter.credit_card_modified, 0)) AS credit_card_modified,
    SUM(COALESCE(metrics.counter.credit_card_saved, 0)) AS credit_card_saved,
    MAX(COALESCE(metrics.quantity.credit_card_saved_all, 0)) AS credit_card_saved_all,
    --Bookmark
    SUM(
      COALESCE(mozfun.map.extract_keyed_scalar_sum(metrics.labeled_counter.bookmarks_add), 0)
    ) AS bookmarks_add,
    SUM(
      COALESCE(mozfun.map.extract_keyed_scalar_sum(metrics.labeled_counter.bookmarks_delete), 0)
    ) AS bookmarks_delete,
    SUM(
      COALESCE(mozfun.map.extract_keyed_scalar_sum(metrics.labeled_counter.bookmarks_edit), 0)
    ) AS bookmarks_edit,
    SUM(
      COALESCE(mozfun.map.extract_keyed_scalar_sum(metrics.labeled_counter.bookmarks_open), 0)
    ) AS bookmarks_open,
    SUM(
      COALESCE(mozfun.map.extract_keyed_scalar_sum(metrics.labeled_counter.bookmarks_view_list), 0)
    ) AS bookmarks_view_list,
    CAST(
      MAX(COALESCE(metrics.boolean.bookmarks_has_mobile_bookmarks, FALSE)) AS INT64
    ) AS has_mobile_bookmarks,
    MAX(COALESCE(metrics.quantity.bookmarks_mobile_bookmarks_count, 0)) AS mobile_bookmarks_count,
    --FxA
    SUM(COALESCE(metrics.counter.sync_create_account_pressed, 0)) AS sync_create_account_pressed,
    SUM(COALESCE(metrics.counter.sync_open_tab, 0)) AS sync_open_tab,
    SUM(COALESCE(metrics.counter.sync_sign_in_sync_pressed, 0)) AS sync_sign_in_sync_pressed,
    --Privacy
    MAX(COALESCE(metrics.quantity.tabs_private_tabs_quantity, 0)) AS tabs_private_tabs_quantity,
    CAST(
      MAX(COALESCE(metrics.boolean.preferences_close_private_tabs, FALSE)) AS INT64
    ) AS preferences_close_private_tabs,
    CAST(
      MAX(COALESCE(metrics.boolean.tracking_protection_enabled, FALSE)) AS INT64
    ) AS tracking_protection_enabled,
    CAST(
      MAX(COALESCE(LOWER(metrics.string.tracking_protection_strength) = "strict", FALSE)) AS INT64
    ) AS tracking_protection_strict_enabled,
    --Tab Count
    MAX(COALESCE(metrics.quantity.tabs_normal_tabs_quantity, 0)) AS tabs_normal_tabs_quantity,
    MAX(COALESCE(metrics.quantity.tabs_inactive_tabs_count, 0)) AS tabs_inactive_tabs_count,
    --Default Browser
    SUM(
      COALESCE(metrics.counter.app_opened_as_default_browser, 0)
    ) AS app_opened_as_default_browser,
    SUM(
      COALESCE(metrics.counter.settings_menu_set_as_default_browser_pressed, 0)
    ) AS settings_menu_set_as_default_browser_pressed,
    --Notification
    CAST(
      MAX(COALESCE(metrics.boolean.preferences_sync_notifs, FALSE)) AS INT64
    ) AS preferences_sync_notifs,
    CAST(
      MAX(COALESCE(metrics.boolean.preferences_tips_and_features_notifs, FALSE)) AS INT64
    ) AS preferences_tips_and_features_notifs,
    --Customize Home
    CAST(
      MAX(COALESCE(metrics.boolean.preferences_jump_back_in, FALSE)) AS INT64
    ) AS preferences_jump_back_in,
    CAST(
      MAX(COALESCE(metrics.boolean.preferences_recently_visited, FALSE)) AS INT64
    ) AS preferences_recently_visited,
    CAST(
      MAX(COALESCE(metrics.boolean.preferences_recently_saved, FALSE)) AS INT64
    ) AS preferences_recently_saved,
    CAST(MAX(COALESCE(metrics.boolean.preferences_pocket, FALSE)) AS INT64) AS preferences_pocket,
    SUM(COALESCE(metrics.counter.app_menu_customize_homepage, 0)) AS app_menu_customize_homepage,
    SUM(
      COALESCE(metrics.counter.firefox_home_page_customize_homepage_button, 0)
    ) AS firefox_home_page_customize_homepage_button,
    --Address
    MAX(COALESCE(metrics.quantity.addresses_saved_all, 0)) AS addresses_saved_all
  FROM
    metrics_dau
  GROUP BY
    dau_date,
    client_id
)
-- Aggregated feature usage
SELECT
  @submission_date AS submission_date,
  dau_date AS metric_date,
  COUNT(DISTINCT client_id) AS clients,
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
  COUNT(
    DISTINCT IF(credit_card_autofill_enabled > 0, client_id, NULL)
  ) AS credit_card_autofill_enabled_users,
  SUM(credit_card_autofill_enabled) AS credit_card_autofill_enabled,
  --credit_card_sync_enabled
  COUNT(
    DISTINCT IF(credit_card_sync_enabled > 0, client_id, NULL)
  ) AS credit_card_sync_enabled_users,
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
  COUNT(
    DISTINCT IF(sync_create_account_pressed > 0, client_id, NULL)
  ) AS sync_create_account_pressed_users,
  SUM(sync_create_account_pressed) AS sync_create_account_pressed,
  --sync_open_tab
  COUNT(DISTINCT IF(sync_open_tab > 0, client_id, NULL)) AS sync_open_tab_users,
  SUM(sync_open_tab) AS sync_open_tab,
  --sync_sign_in_sync_pressed
  COUNT(
    DISTINCT IF(sync_sign_in_sync_pressed > 0, client_id, NULL)
  ) AS sync_sign_in_sync_pressed_users,
  SUM(sync_sign_in_sync_pressed) AS sync_sign_in_sync_pressed,
  /*Privacy*/
  --tabs_private_tabs_quantity
  COUNT(
    DISTINCT IF(tabs_private_tabs_quantity > 0, client_id, NULL)
  ) AS tabs_private_tabs_quantity_users,
  SUM(tabs_private_tabs_quantity) AS tabs_private_tabs_quantity,
  -- Preferences Close Private Tabs
  COUNT(
    DISTINCT IF(preferences_close_private_tabs > 0, client_id, NULL)
  ) AS preferences_close_private_tabs_users,
  SUM(preferences_close_private_tabs) AS preferences_close_private_tabs,
  -- Tracking Protection Enabled
  COUNT(
    DISTINCT IF(tracking_protection_enabled > 0, client_id, NULL)
  ) AS tracking_protection_enabled_users,
  SUM(tracking_protection_enabled) AS tracking_protection_enabled,
  -- Tracking Protection Strict
  COUNT(
    DISTINCT IF(tracking_protection_strict_enabled > 0, client_id, NULL)
  ) AS tracking_protection_strict_users,
  SUM(tracking_protection_strict_enabled) AS tracking_protection_strict,
  /*Tab Count*/
  --tabs_normal_tabs_quantity
  COUNT(
    DISTINCT IF(tabs_normal_tabs_quantity > 0, client_id, NULL)
  ) AS tabs_normal_tabs_quantity_users,
  SUM(tabs_normal_tabs_quantity) AS tabs_normal_tabs_quantity,
  --tabs_inactive_tabs_count
  COUNT(
    DISTINCT IF(tabs_inactive_tabs_count > 0, client_id, NULL)
  ) AS tabs_inactive_tabs_count_users,
  SUM(tabs_inactive_tabs_count) AS tabs_inactive_tabs_count,
  /*Default Browser*/
  --app_opened_as_default_browser
  COUNT(
    DISTINCT IF(app_opened_as_default_browser > 0, client_id, NULL)
  ) AS app_opened_as_default_browser_users,
  SUM(app_opened_as_default_browser) AS app_opened_as_default_browser,
  -- settings_menu_set_as_default_browser_pressed
  COUNT(
    DISTINCT IF(settings_menu_set_as_default_browser_pressed > 0, client_id, NULL)
  ) AS settings_menu_set_as_default_browser_pressed_users,
  SUM(settings_menu_set_as_default_browser_pressed) AS settings_menu_set_as_default_browser_pressed,
  /*Notification*/
  --preferences_sync_notifs
  COUNT(DISTINCT IF(preferences_sync_notifs > 0, client_id, NULL)) AS preferences_sync_notifs_users,
  SUM(preferences_sync_notifs) AS preferences_sync_notifs,
  -- preferences_tips_and_features_notifs
  COUNT(
    DISTINCT IF(preferences_tips_and_features_notifs > 0, client_id, NULL)
  ) AS preferences_tips_and_features_notifs_users,
  SUM(preferences_tips_and_features_notifs) AS preferences_tips_and_features_notifs,
  /*Customize Home*/
  --preferences_jump_back_in
  COUNT(
    DISTINCT IF(preferences_jump_back_in > 0, client_id, NULL)
  ) AS preferences_jump_back_in_users,
  SUM(preferences_jump_back_in) AS preferences_jump_back_in,
  -- Preferences Recently Visited
  COUNT(
    DISTINCT IF(preferences_recently_visited > 0, client_id, NULL)
  ) AS preferences_recently_visited_users,
  SUM(preferences_recently_visited) AS preferences_recently_visited,
  -- Preferences Recently Saved
  COUNT(
    DISTINCT IF(preferences_recently_saved > 0, client_id, NULL)
  ) AS preferences_recently_saved_users,
  SUM(preferences_recently_saved) AS preferences_recently_saved,
  -- Preferences Pocket
  COUNT(DISTINCT IF(preferences_pocket > 0, client_id, NULL)) AS preferences_pocket_users,
  SUM(preferences_pocket) AS preferences_pocket,
  -- App Menu Customize Homepage
  COUNT(
    DISTINCT IF(app_menu_customize_homepage > 0, client_id, NULL)
  ) AS app_menu_customize_homepage_users,
  SUM(app_menu_customize_homepage) AS app_menu_customize_homepage,
  -- Firefox Home Page Customize Homepage Button
  COUNT(
    DISTINCT IF(firefox_home_page_customize_homepage_button > 0, client_id, NULL)
  ) AS firefox_home_page_customize_homepage_button_users,
  SUM(firefox_home_page_customize_homepage_button) AS firefox_home_page_customize_homepage_button,
  -- addresses_saved_all
  COUNT(DISTINCT IF(addresses_saved_all > 0, client_id, NULL)) AS addresses_saved_all_users,
  SUM(addresses_saved_all) AS addresses_saved_all
FROM
  metric_ping_clients_feature_usage
LEFT JOIN
  client_attribution
  USING (client_id)
WHERE
  dau_date = DATE_SUB(@submission_date, INTERVAL 4 DAY)
GROUP BY
  submission_date,
  metric_date,
  channel,
  country,
  adjust_network,
  is_default_browser
