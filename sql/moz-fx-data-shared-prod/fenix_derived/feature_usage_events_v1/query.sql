-- Query for fenix_derived.feature_usage_events_v1
-- For more information on writing queries see:
-- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
DECLARE submission_date DATE DEFAULT "2023-12-07";

WITH events_ping_distinct_client_count AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    COUNT(DISTINCT client_info.client_id) AS events_ping_client_count
  FROM
    fenix.events_unnested
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
    --autofill
    COUNTIF(event_category = 'logins' AND event_name = 'password_detected') AS autofill_password_detected_logins,
    COUNTIF(event_category = 'logins' AND event_name = 'autofill_prompt_shown') AS autofill_prompt_shown_logins,
    COUNTIF(event_category = 'logins' AND event_name = 'autofill_prompt_dismissed') AS autofill_prompt_dismissed_logins,
    COUNTIF(event_category = 'logins' AND event_name = 'autofilled') AS autofilled_logins,
    --management
    COUNTIF(event_category = 'logins' AND event_name = 'management_add_tapped') AS management_add_tapped_logins,
    COUNTIF(event_category = 'logins' AND event_name = 'management_logins_tapped') AS management_tapped_logins,
    /*Credit Card*/
    --autofill
    COUNTIF(event_category = 'credit_cards' AND event_name = 'form_detected') AS form_detected_cc,
    COUNTIF(event_category = 'credit_cards' AND event_name = 'autofill_prompt_shown') AS autofill_prompt_shown_cc,
    COUNTIF(event_category = 'credit_cards' AND event_name = 'autofill_prompt_expanded') AS autofill_prompt_expanded_cc,
    COUNTIF(event_category = 'credit_cards' AND event_name = 'autofill_prompt_dismissed') AS autofill_prompt_dismissed_cc,
    COUNTIF(event_category = 'credit_cards' AND event_name = 'autofilled') AS autofilled_cc,
    --save prompt
    COUNTIF(event_category = 'credit_cards' AND event_name = 'save_prompt_shown') AS save_prompt_shown_cc,
    COUNTIF(event_category = 'credit_cards' AND event_name = 'save_prompt_create') AS save_prompt_create_cc,
    COUNTIF(event_category = 'credit_cards' AND event_name = 'save_prompt_update') AS save_prompt_update_cc,
    --management
    COUNTIF(event_category = 'credit_cards' AND event_name = 'management_add_tapped') AS management_add_tapped_cc,
    COUNTIF(event_category = 'credit_cards' AND event_name = 'management_card_tapped') AS management_tapped_cc,
    COUNTIF(event_category = 'credit_cards' AND event_name = 'modified') AS modified_cc,
    /*Addresses*/
    --autofill
    COUNTIF(event_category = 'addresses' AND event_name = 'form_detected') AS form_detected_address,
    COUNTIF(event_category = 'addresses' AND event_name = 'autofill_prompt_shown') AS autofill_prompt_shown_address,
    COUNTIF(event_category = 'addresses' AND event_name = 'autofill_prompt_expanded') AS autofill_prompt_expanded_address,
    COUNTIF(event_category = 'addresses' AND event_name = 'autofill_prompt_dismissed') AS autofill_prompt_dismissed_address,
    COUNTIF(event_category = 'addresses' AND event_name = 'autofilled') AS autofilled_address,
    --management
    COUNTIF(event_category = 'addresses' AND event_name = 'management_add_tapped') AS management_add_tapped_address,
    COUNTIF(event_category = 'addresses' AND event_name = 'management_address_tapped') AS management_tapped_address,
    /*Bookmark*/
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'copied') AS bookmark_copied,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'edited') AS bookmark_edited,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'folder_add') AS bookmark_folder_add,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'open') AS bookmark_open,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'open_all_in_new_tabs') AS bookmark_open_all_in_new_tabs,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'open_all_in_private_tabs') AS bookmark_open_all_in_private_tabs,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'open_in_new_tab') AS bookmark_open_in_new_tab,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'open_in_new_tabs') AS bookmark_open_in_new_tabs,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'open_in_private_tab') AS bookmark_open_in_private_tab,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'open_in_private_tabs') AS bookmark_open_in_private_tabs,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'removed') AS bookmark_removed,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'search_icon_tapped') AS bookmark_search_icon_tapped,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'search_result_tapped') AS bookmark_search_result_tapped,
    COUNTIF(event_category = 'bookmarks_management' AND event_name = 'shared') AS bookmark_shared,
    /*History*/
    COUNTIF(event_category = 'history' AND event_name = 'opened') AS history_opened,
    COUNTIF(event_category = 'history' AND event_name = 'opened_item') AS history_opened_item,
    COUNTIF(event_category = 'history' AND event_name = 'opened_items_in_new_tabs') AS history_opened_items_in_new_tabs,
    COUNTIF(event_category = 'history' AND event_name = 'opened_items_in_private_tabs') AS history_opened_items_in_private_tabs,
    COUNTIF(event_category = 'history' AND event_name = 'recent_searches_tapped') AS history_recent_searches_tapped,
    COUNTIF(event_category = 'history' AND event_name = 'remove_prompt_cancelled') AS history_remove_prompt_cancelled,
    COUNTIF(event_category = 'history' AND event_name = 'remove_prompt_opened') AS history_remove_prompt_opened,
    COUNTIF(event_category = 'history' AND event_name = 'removed') AS history_removed,
    COUNTIF(event_category = 'history' AND event_name = 'removed_all') AS history_removed_all,
    COUNTIF(event_category = 'history' AND event_name = 'removed_last_hour') AS history_removed_last_hour,
    COUNTIF(event_category = 'history' AND event_name = 'removed_today_and_yesterday') AS history_removed_today_and_yesterday,
    COUNTIF(event_category = 'history' AND event_name = 'search_icon_tapped') AS history_search_icon_tapped,
    COUNTIF(event_category = 'history' AND event_name = 'search_result_tapped') AS history_search_result_tapped,
    COUNTIF(event_category = 'history' AND event_name = 'search_term_group_open_tab') AS history_search_term_group_open_tab,
    COUNTIF(event_category = 'history' AND event_name = 'search_term_group_remove_all') AS history_search_term_group_remove_all,
    COUNTIF(event_category = 'history' AND event_name = 'search_term_group_remove_tab') AS history_search_term_group_remove_tab,
    COUNTIF(event_category = 'history' AND event_name = 'search_term_group_tapped') AS history_search_term_group_tapped,
    COUNTIF(event_category = 'history' AND event_name = 'shared') AS history_shared,
    /*FxA*/
    COUNTIF(event_category = 'sync' AND event_name = 'failed') AS sync_failed,
    COUNTIF(event_category = 'sync_account' AND event_name = 'opened') AS sync_account_opened,
    COUNTIF(event_category = 'sync_account' AND event_name = 'send_tab') AS sync_account_send_tab,
    COUNTIF(event_category = 'sync_account' AND event_name = 'sign_in_to_send_tab') AS sync_account_sign_in_to_send_tab,
    COUNTIF(event_category = 'sync_account' AND event_name = 'sync_now') AS sync_account_sync_now,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'closed') AS sync_auth_closed,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'opened') AS sync_auth_opened,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'other_external') AS sync_auth_other_external,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'paired') AS sync_auth_paired,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'recovered') AS sync_auth_recovered,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'scan_pairing') AS sync_auth_scan_pairing,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'sign_in') AS sync_auth_sign_in,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'sign_out') AS sync_auth_sign_out,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'sign_up') AS sync_auth_sign_up,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'use_email') AS sync_auth_use_email,
    COUNTIF(event_category = 'sync_auth' AND event_name = 'use_email_problem') AS sync_auth_use_email_problem,
    /*Privacy*/
    COUNTIF(event_category = 'homepage' AND event_name = 'private_mode_icon_tapped') AS hp_private_mode_tapped,
    COUNTIF(event_category = 'tabs_tray' AND event_name = 'private_mode_tapped') AS tab_tray_private_mode_switched,
    COUNTIF(event_category = 'app_icon' AND event_name = 'new_private_tab_tapped') AS app_icon_private_tab_tapped,
    COUNTIF(event_category = 'tabs_tray' AND event_name = 'new_private_tab_tapped') AS tab_tray_private_mode_tapped,
    --etp
    COUNTIF(event_category = 'tracking_protection' AND event_name = 'etp_setting_changed') AS etp_setting_changed,
    COUNTIF(event_category = 'tracking_protection' AND event_name = 'etp_settings') AS etp_settings,
    COUNTIF(event_category = 'tracking_protection' AND event_name = 'etp_shield') AS etp_shield,
    COUNTIF(event_category = 'tracking_protection' AND event_name = 'etp_tracker_list') AS etp_tracker_list,
    /*Default browser*/
    COUNTIF(event_category = 'events' AND event_name = 'default_browser_changed') AS default_browser_changed,
    /*Notification*/
    COUNTIF(event_category = 'events' AND event_name = 're_engagement_notif_shown') AS re_engagement_notif_shown,
    COUNTIF(event_category = 'events' AND event_name = 're_engagement_notif_tapped') AS re_engagement_notif_tapped,
    /*Customize Home*/
    COUNTIF(event_category = 'app_menu' AND event_name = 'customize_homepage') AS app_menu_customize_homepage,
    COUNTIF(event_category = 'home_screen' AND event_name = 'customize_home_clicked') AS home_page_customize_home_clicked
  FROM
    fenix.events_unnested
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
--autofill_prompt_shown
    SUM(autofill_password_detected_logins) AS autofill_password_detected_logins,
    COUNT(
      DISTINCT
      CASE
        WHEN autofill_password_detected_logins > 0
          THEN client_id
      END
    ) AS autofill_password_detected_users_logins,
    SUM(autofill_prompt_shown_logins) AS autofill_prompt_shown_sum_logins,
    COUNT(
      DISTINCT
      CASE
        WHEN autofill_prompt_shown_logins > 0
          THEN client_id
      END
    ) AS autofill_prompt_shown_users_logins,
--autofill_prompt_dismissed
    SUM(autofill_prompt_dismissed_logins) AS autofill_prompt_dismissed_sum_logins,
    COUNT(
      DISTINCT
      CASE
        WHEN autofill_prompt_dismissed_logins > 0
          THEN client_id
      END
    ) AS autofill_prompt_dismissed_users_logins
--autofilled
    ,
    SUM(autofilled_logins) AS autofilled_sum_logins,
    COUNT(DISTINCT CASE WHEN autofilled_logins > 0 THEN client_id END) AS autofilled_users_logins,
--management_add_tapped
    SUM(management_add_tapped_logins) AS management_add_tapped_sum_logins,
    COUNT(
      DISTINCT
      CASE
        WHEN management_add_tapped_logins > 0
          THEN client_id
      END
    ) AS management_add_tapped_users_logins,
--management_card_tapped
    SUM(management_tapped_logins) AS management_tapped_sum_logins,
    COUNT(
      DISTINCT
      CASE
        WHEN management_tapped_logins > 0
          THEN client_id
      END
    ) AS management_tapped_users_logins,
/*Credit Cards*/
--form detected
    SUM(form_detected_cc) AS form_detected_sum_cc,
    COUNT(DISTINCT CASE WHEN form_detected_cc > 0 THEN client_id END) AS form_detected_users_cc,
--autofill_prompt_shown
    SUM(autofill_prompt_shown_cc) AS autofill_prompt_shown_sum_cc,
    COUNT(
      DISTINCT
      CASE
        WHEN autofill_prompt_shown_cc > 0
          THEN client_id
      END
    ) AS autofill_prompt_shown_users_cc,
--autofill_prompt_expanded
    SUM(autofill_prompt_expanded_cc) AS autofill_prompt_expanded_sum_cc,
    COUNT(
      DISTINCT
      CASE
        WHEN autofill_prompt_expanded_cc > 0
          THEN client_id
      END
    ) AS autofill_prompt_expanded_users_cc,
--autofill_prompt_dismissed
    SUM(autofill_prompt_dismissed_cc) AS autofill_prompt_dismissed_sum_cc,
    COUNT(
      DISTINCT
      CASE
        WHEN autofill_prompt_dismissed_cc > 0
          THEN client_id
      END
    ) AS autofill_prompt_dismissed_users_cc,
--autofilled
    SUM(autofilled_cc) AS autofilled_sum_cc,
    COUNT(DISTINCT CASE WHEN autofilled_cc > 0 THEN client_id END) AS autofilled_users_cc,
--save_prompt_shown
    SUM(save_prompt_shown_cc) AS save_prompt_shown_sum_cc,
    COUNT(
      DISTINCT
      CASE
        WHEN save_prompt_shown_cc > 0
          THEN client_id
      END
    ) AS save_prompt_shown_users_cc,
--save_prompt_create
    SUM(save_prompt_create_cc) AS save_prompt_create_sum_cc,
    COUNT(
      DISTINCT
      CASE
        WHEN save_prompt_create_cc > 0
          THEN client_id
      END
    ) AS save_prompt_create_users_cc,
--save_prompt_update
    SUM(save_prompt_update_cc) AS save_prompt_update_sum_cc,
    COUNT(
      DISTINCT
      CASE
        WHEN save_prompt_update_cc > 0
          THEN client_id
      END
    ) AS save_prompt_update_users_cc,
--management_add_tapped
    SUM(management_add_tapped_cc) AS management_add_tapped_sum_cc,
    COUNT(
      DISTINCT
      CASE
        WHEN management_add_tapped_cc > 0
          THEN client_id
      END
    ) AS management_add_tapped_users_cc,
--management_card_tapped
    SUM(management_tapped_cc) AS management_tapped_sum_cc,
    COUNT(
      DISTINCT
      CASE
        WHEN management_tapped_cc > 0
          THEN client_id
      END
    ) AS management_tapped_users_cc,
--modified
    SUM(modified_cc) AS modified_sum_cc,
    COUNT(DISTINCT CASE WHEN modified_cc > 0 THEN client_id END) AS modified_users_cc,
/*Addresses*/
--form detected
    SUM(form_detected_address) AS form_detected_sum_address,
    COUNT(
      DISTINCT
      CASE
        WHEN form_detected_address > 0
          THEN client_id
      END
    ) AS form_detected_users_address,
--autofill_prompt_shown
    SUM(autofill_prompt_shown_address) AS autofill_prompt_shown_sum_address,
    COUNT(
      DISTINCT
      CASE
        WHEN autofill_prompt_shown_address > 0
          THEN client_id
      END
    ) AS autofill_prompt_shown_users_address,
--autofill_prompt_expanded
    SUM(autofill_prompt_expanded_address) AS autofill_prompt_expanded_sum_address,
    COUNT(
      DISTINCT
      CASE
        WHEN autofill_prompt_expanded_address > 0
          THEN client_id
      END
    ) AS autofill_prompt_expanded_users_address,
--autofill_prompt_dismissed
    SUM(autofill_prompt_dismissed_address) AS autofill_prompt_dismissed_sum_address,
    COUNT(
      DISTINCT
      CASE
        WHEN autofill_prompt_dismissed_address > 0
          THEN client_id
      END
    ) AS autofill_prompt_dismissed_users_address,
--autofilled
    SUM(autofilled_address) AS autofilled_sum_address,
    COUNT(DISTINCT CASE WHEN autofilled_address > 0 THEN client_id END) AS autofilled_users_address,
--management_add_tapped
    SUM(management_add_tapped_address) AS management_add_tapped_sum_address,
    COUNT(
      DISTINCT
      CASE
        WHEN management_add_tapped_address > 0
          THEN client_id
      END
    ) AS management_add_tapped_users_address,
--management_card_tapped
    SUM(management_tapped_address) AS management_tapped_sum_address,
    COUNT(
      DISTINCT
      CASE
        WHEN management_tapped_address > 0
          THEN client_id
      END
    ) AS management_tapped_users_address,
/*Bookmark*/
--copied
    SUM(bookmark_copied) AS bookmark_copied,
    COUNT(DISTINCT CASE WHEN bookmark_copied > 0 THEN client_id END) AS bookmark_copied_users,
--edited
    SUM(bookmark_edited) AS bookmark_edited,
    COUNT(DISTINCT CASE WHEN bookmark_edited > 0 THEN client_id END) AS bookmark_edited_users,
--folder_add
    SUM(bookmark_folder_add) AS bookmark_folder_add,
    COUNT(
      DISTINCT
      CASE
        WHEN bookmark_folder_add > 0
          THEN client_id
      END
    ) AS bookmark_folder_add_users,
--open
    SUM(bookmark_open) AS bookmark_open,
    COUNT(DISTINCT CASE WHEN bookmark_open > 0 THEN client_id END) AS bookmark_open_users,
--open_all_in_new_tabs
    SUM(bookmark_open_all_in_new_tabs) AS bookmark_open_all_in_new_tabs,
    COUNT(
      DISTINCT
      CASE
        WHEN bookmark_open_all_in_new_tabs > 0
          THEN client_id
      END
    ) AS bookmark_open_all_in_new_tabs_users,
--open_all_in_private_tabs
    SUM(bookmark_open_all_in_private_tabs) AS bookmark_open_all_in_private_tabs,
    COUNT(
      DISTINCT
      CASE
        WHEN bookmark_open_all_in_private_tabs > 0
          THEN client_id
      END
    ) AS bookmark_open_all_in_private_tabs_users,
--open_in_new_tab
    SUM(bookmark_open_in_new_tab) AS bookmark_open_in_new_tab,
    COUNT(
      DISTINCT
      CASE
        WHEN bookmark_open_in_new_tab > 0
          THEN client_id
      END
    ) AS bookmark_open_in_new_tab_users,
--open_in_new_tabs
    SUM(bookmark_open_in_new_tabs) AS bookmark_open_in_new_tabs,
    COUNT(
      DISTINCT
      CASE
        WHEN bookmark_open_in_new_tabs > 0
          THEN client_id
      END
    ) AS bookmark_open_in_new_tabs_users,
--open_in_private_tab
    SUM(bookmark_open_in_private_tab) AS bookmark_open_in_private_tab,
    COUNT(
      DISTINCT
      CASE
        WHEN bookmark_open_in_private_tab > 0
          THEN client_id
      END
    ) AS bookmark_open_in_private_tab_users,
--open_in_private_tabs
    SUM(bookmark_open_in_private_tabs) AS bookmark_open_in_private_tabs,
    COUNT(
      DISTINCT
      CASE
        WHEN bookmark_open_in_private_tabs > 0
          THEN client_id
      END
    ) AS bookmark_open_in_private_tabs_users,
--removed
    SUM(bookmark_removed) AS bookmark_removed,
    COUNT(DISTINCT CASE WHEN bookmark_removed > 0 THEN client_id END) AS bookmark_removed_users,
--search_icon_tapped
    SUM(bookmark_search_icon_tapped) AS bookmark_search_icon_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN bookmark_search_icon_tapped > 0
          THEN client_id
      END
    ) AS bookmark_search_icon_tapped_users,
--search_result_tapped
    SUM(bookmark_search_result_tapped) AS bookmark_search_result_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN bookmark_search_result_tapped > 0
          THEN client_id
      END
    ) AS bookmark_search_result_tapped_users,
--shared
    SUM(bookmark_shared) AS bookmark_shared,
    COUNT(DISTINCT CASE WHEN bookmark_shared > 0 THEN client_id END) AS bookmark_shared_users,
/*History*/
--opened
    SUM(history_opened) AS history_opened,
    COUNT(DISTINCT CASE WHEN history_opened > 0 THEN client_id END) AS history_opened_users,
--opened_item
    SUM(history_opened_item) AS history_opened_item,
    COUNT(
      DISTINCT
      CASE
        WHEN history_opened_item > 0
          THEN client_id
      END
    ) AS history_opened_item_users,
--opened_items_in_new_tabs
    SUM(history_opened_items_in_new_tabs) AS history_opened_items_in_new_tabs,
    COUNT(
      DISTINCT
      CASE
        WHEN history_opened_items_in_new_tabs > 0
          THEN client_id
      END
    ) AS history_opened_items_in_new_tabs_users,
--opened_items_in_private_tabs
    SUM(history_opened_items_in_private_tabs) AS history_opened_items_in_private_tabs,
    COUNT(
      DISTINCT
      CASE
        WHEN history_opened_items_in_private_tabs > 0
          THEN client_id
      END
    ) AS history_opened_items_in_private_tabs_users,
--recent_searches_tapped
    SUM(history_recent_searches_tapped) AS history_recent_searches_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN history_recent_searches_tapped > 0
          THEN client_id
      END
    ) AS history_recent_searches_tapped_users,
--remove_prompt_cancelled
    SUM(history_remove_prompt_cancelled) AS history_remove_prompt_cancelled,
    COUNT(
      DISTINCT
      CASE
        WHEN history_remove_prompt_cancelled > 0
          THEN client_id
      END
    ) AS history_remove_prompt_cancelled_users,
--remove_prompt_opened
    SUM(history_remove_prompt_opened) AS history_remove_prompt_opened,
    COUNT(
      DISTINCT
      CASE
        WHEN history_remove_prompt_opened > 0
          THEN client_id
      END
    ) AS history_remove_prompt_opened_users,
--removed
    SUM(history_removed) AS history_removed,
    COUNT(DISTINCT CASE WHEN history_removed > 0 THEN client_id END) AS history_removed_users,
--removed_all
    SUM(history_removed_all) AS history_removed_all,
    COUNT(
      DISTINCT
      CASE
        WHEN history_removed_all > 0
          THEN client_id
      END
    ) AS history_removed_all_users,
--removed_last_hour
    SUM(history_removed_last_hour) AS history_removed_last_hour,
    COUNT(
      DISTINCT
      CASE
        WHEN history_removed_last_hour > 0
          THEN client_id
      END
    ) AS history_removed_last_hour_users,
--removed_today_and_yesterday
    SUM(history_removed_today_and_yesterday) AS history_removed_today_and_yesterday,
    COUNT(
      DISTINCT
      CASE
        WHEN history_removed_today_and_yesterday > 0
          THEN client_id
      END
    ) AS history_removed_today_and_yesterday_users,
--search_icon_tapped
    SUM(history_search_icon_tapped) AS history_search_icon_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN history_search_icon_tapped > 0
          THEN client_id
      END
    ) AS history_search_icon_tapped_users,
--search_term_group_open_tab
    SUM(history_search_term_group_open_tab) AS history_search_term_group_open_tab,
    COUNT(
      DISTINCT
      CASE
        WHEN history_search_term_group_open_tab > 0
          THEN client_id
      END
    ) AS history_search_term_group_open_tab_users,
--search_term_group_remove_all
    SUM(history_search_term_group_remove_all) AS history_search_term_group_remove_all,
    COUNT(
      DISTINCT
      CASE
        WHEN history_search_term_group_remove_all > 0
          THEN client_id
      END
    ) AS history_search_term_group_remove_all_users,
--search_term_group_remove_tab
    SUM(history_search_term_group_remove_tab) AS history_search_term_group_remove_tab,
    COUNT(
      DISTINCT
      CASE
        WHEN history_search_term_group_remove_tab > 0
          THEN client_id
      END
    ) AS history_search_term_group_remove_tab_users,
--search_term_group_tapped
    SUM(history_search_term_group_tapped) AS history_search_term_group_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN history_search_term_group_tapped > 0
          THEN client_id
      END
    ) AS history_search_term_group_tapped_users,
--shared
    SUM(history_shared) AS history_shared,
    COUNT(DISTINCT CASE WHEN history_shared > 0 THEN client_id END) AS history_shared_users,
/*FxA*/
--sync_failed
    SUM(sync_failed) AS sync_failed,
    COUNT(DISTINCT CASE WHEN sync_failed > 0 THEN client_id END) AS sync_failed_users,
--sync_account_opened
    SUM(sync_account_opened) AS sync_account_opened,
    COUNT(
      DISTINCT
      CASE
        WHEN sync_account_opened > 0
          THEN client_id
      END
    ) AS sync_account_opened_users,
--sync_account_send_tab
    SUM(sync_account_send_tab) AS sync_account_send_tab,
    COUNT(
      DISTINCT
      CASE
        WHEN sync_account_send_tab > 0
          THEN client_id
      END
    ) AS sync_account_send_tab_users,
--sync_account_sign_in_to_send_tab
    SUM(sync_account_sign_in_to_send_tab) AS sync_account_sign_in_to_send_tab,
    COUNT(
      DISTINCT
      CASE
        WHEN sync_account_sign_in_to_send_tab > 0
          THEN client_id
      END
    ) AS sync_account_sign_in_to_send_tab_users,
--sync_account_sync_now
    SUM(sync_account_sync_now) AS sync_account_sync_now,
    COUNT(
      DISTINCT
      CASE
        WHEN sync_account_sync_now > 0
          THEN client_id
      END
    ) AS sync_account_sync_now_users,
--sync_auth_closed
    SUM(sync_auth_closed) AS sync_auth_closed,
    COUNT(DISTINCT CASE WHEN sync_auth_closed > 0 THEN client_id END) AS sync_auth_closed_users,
--sync_auth_opened
    SUM(sync_auth_opened) AS sync_auth_opened,
    COUNT(DISTINCT CASE WHEN sync_auth_opened > 0 THEN client_id END) AS sync_auth_opened_users,
--sync_auth_other_external
    SUM(sync_auth_other_external) AS sync_auth_other_external,
    COUNT(
      DISTINCT
      CASE
        WHEN sync_auth_other_external > 0
          THEN client_id
      END
    ) AS sync_auth_other_external_users,
--sync_auth_paired
    SUM(sync_auth_paired) AS sync_auth_paired,
    COUNT(DISTINCT CASE WHEN sync_auth_paired > 0 THEN client_id END) AS sync_auth_paired_users,
--sync_auth_recovered
    SUM(sync_auth_recovered) AS sync_auth_recovered,
    COUNT(
      DISTINCT
      CASE
        WHEN sync_auth_recovered > 0
          THEN client_id
      END
    ) AS sync_auth_recovered_users,
--sync_auth_scan_pairing
    SUM(sync_auth_scan_pairing) AS sync_auth_scan_pairing,
    COUNT(
      DISTINCT
      CASE
        WHEN sync_auth_scan_pairing > 0
          THEN client_id
      END
    ) AS sync_auth_scan_pairing_users,
--sync_auth_sign_in
    SUM(sync_auth_sign_in) AS sync_auth_sign_in,
    COUNT(DISTINCT CASE WHEN sync_auth_sign_in > 0 THEN client_id END) AS sync_auth_sign_in_users,
--sync_auth_sign_out
    SUM(sync_auth_sign_out) AS sync_auth_sign_out,
    COUNT(DISTINCT CASE WHEN sync_auth_sign_out > 0 THEN client_id END) AS sync_auth_sign_out_users,
--sync_auth_sign_up
    SUM(sync_auth_sign_up) AS sync_auth_sign_up,
    COUNT(DISTINCT CASE WHEN sync_auth_sign_up > 0 THEN client_id END) AS sync_auth_sign_up_users,
--sync_auth_use_email
    SUM(sync_auth_use_email) AS sync_auth_use_email,
    COUNT(
      DISTINCT
      CASE
        WHEN sync_auth_use_email > 0
          THEN client_id
      END
    ) AS sync_auth_use_email_users,
--sync_auth_use_email_problem
    SUM(sync_auth_use_email_problem) AS sync_auth_use_email_problem,
    COUNT(
      DISTINCT
      CASE
        WHEN sync_auth_use_email_problem > 0
          THEN client_id
      END
    ) AS sync_auth_use_email_problem_users,
/*Privacy*/
--hp_private_mode_tapped
    SUM(hp_private_mode_tapped) AS hp_private_mode_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN hp_private_mode_tapped > 0
          THEN client_id
      END
    ) AS hp_private_mode_tapped_users,
--tab_tray_private_mode_switched
    SUM(tab_tray_private_mode_switched) AS tab_tray_private_mode_switched,
    COUNT(
      DISTINCT
      CASE
        WHEN tab_tray_private_mode_switched > 0
          THEN client_id
      END
    ) AS tab_tray_private_mode_switched_users,
--app_icon_private_tab_tapped
    SUM(app_icon_private_tab_tapped) AS app_icon_private_tab_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN app_icon_private_tab_tapped > 0
          THEN client_id
      END
    ) AS app_icon_private_tab_tapped_users,
--tab_tray_private_mode_tapped
    SUM(tab_tray_private_mode_tapped) AS tab_tray_private_mode_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN tab_tray_private_mode_tapped > 0
          THEN client_id
      END
    ) AS tab_tray_private_mode_tapped_users,
--etp_setting_changed
    SUM(etp_setting_changed) AS etp_setting_changed,
    COUNT(
      DISTINCT
      CASE
        WHEN etp_setting_changed > 0
          THEN client_id
      END
    ) AS etp_setting_changed_users,
--etp_settings
    SUM(etp_settings) AS etp_settings,
    COUNT(DISTINCT CASE WHEN etp_settings > 0 THEN client_id END) AS etp_settings_users,
--etp_shield
    SUM(etp_shield) AS etp_shield,
    COUNT(DISTINCT CASE WHEN etp_shield > 0 THEN client_id END) AS etp_shield_users,
--etp_tracker_list
    SUM(etp_tracker_list) AS etp_tracker_list,
    COUNT(DISTINCT CASE WHEN etp_tracker_list > 0 THEN client_id END) AS etp_tracker_list_users,
/*Default browser*/
--default_browser_changed
    COUNT(
      DISTINCT
      CASE
        WHEN default_browser_changed > 0
          THEN client_id
      END
    ) AS default_browser_changed_users,
    SUM(default_browser_changed) AS default_browser_changed,
/*Notification*/
--re_engagement_notif_shown
    SUM(re_engagement_notif_shown) AS re_engagement_notif_shown,
    COUNT(
      DISTINCT
      CASE
        WHEN re_engagement_notif_shown > 0
          THEN client_id
      END
    ) AS re_engagement_notif_shown_users,
--re_engagement_notif_tapped
    SUM(re_engagement_notif_tapped) AS re_engagement_notif_tapped,
    COUNT(
      DISTINCT
      CASE
        WHEN re_engagement_notif_tapped > 0
          THEN client_id
      END
    ) AS re_engagement_notif_tapped_users,
/*Customize Home*/
--app_menu_customize_homepage
    SUM(app_menu_customize_homepage) AS app_menu_customize_homepage,
    COUNT(
      DISTINCT
      CASE
        WHEN app_menu_customize_homepage > 0
          THEN client_id
      END
    ) AS app_menu_customize_homepage_users,
--home_page_customize_home_clicked
    SUM(home_page_customize_home_clicked) AS home_page_customize_home_clicked,
    COUNT(
      DISTINCT
      CASE
        WHEN home_page_customize_home_clicked > 0
          THEN client_id
      END
    ) AS home_page_customize_home_clicked_users
  FROM
    client_product_feature_usage
  GROUP BY
    submission_date
)
SELECT
  submission_date,
  events_ping_client_count,
/*logins*/
  autofill_password_detected_logins,
  autofill_password_detected_users_logins,
  autofill_prompt_shown_sum_logins,
  autofill_prompt_shown_users_logins,
  autofill_prompt_dismissed_sum_logins,
  autofill_prompt_dismissed_users_logins,
  autofilled_sum_logins,
  autofilled_users_logins,
  management_add_tapped_sum_logins,
  management_add_tapped_users_logins,
  management_tapped_sum_logins,
  management_tapped_users_logins,
/*credit card*/
  form_detected_sum_cc,
  form_detected_users_cc,
  autofill_prompt_shown_sum_cc,
  autofill_prompt_shown_users_cc,
  autofill_prompt_expanded_sum_cc,
  autofill_prompt_expanded_users_cc,
  autofill_prompt_dismissed_sum_cc,
  autofill_prompt_dismissed_users_cc,
  autofilled_sum_cc,
  autofilled_users_cc,
  save_prompt_shown_sum_cc,
  save_prompt_shown_users_cc,
  save_prompt_create_sum_cc,
  save_prompt_create_users_cc,
  save_prompt_update_sum_cc,
  save_prompt_update_users_cc,
  management_add_tapped_sum_cc,
  management_add_tapped_users_cc,
  management_tapped_sum_cc,
  management_tapped_users_cc,
/*addresses*/
  form_detected_sum_address,
  form_detected_users_address,
  autofill_prompt_shown_sum_address,
  autofill_prompt_shown_users_address,
  autofill_prompt_expanded_sum_address,
  autofill_prompt_expanded_users_address,
  autofill_prompt_dismissed_sum_address,
  autofill_prompt_dismissed_users_address,
  autofilled_sum_address,
  autofilled_users_address,
  management_add_tapped_sum_address,
  management_add_tapped_users_address,
  management_tapped_sum_address,
  management_tapped_users_address,
/*Bookmark*/
  bookmark_copied,
  bookmark_copied_users,
  bookmark_edited,
  bookmark_edited_users,
  bookmark_folder_add,
  bookmark_folder_add_users,
  bookmark_open,
  bookmark_open_users,
  bookmark_open_all_in_new_tabs,
  bookmark_open_all_in_new_tabs_users,
  bookmark_open_all_in_private_tabs,
  bookmark_open_all_in_private_tabs_users,
  bookmark_open_in_new_tab,
  bookmark_open_in_new_tab_users,
  bookmark_open_in_new_tabs,
  bookmark_open_in_new_tabs_users,
  bookmark_open_in_private_tab,
  bookmark_open_in_private_tab_users,
  bookmark_open_in_private_tabs,
  bookmark_open_in_private_tabs_users,
  bookmark_removed,
  bookmark_removed_users,
  bookmark_search_icon_tapped,
  bookmark_search_icon_tapped_users,
  bookmark_search_result_tapped,
  bookmark_search_result_tapped_users,
  bookmark_shared,
  bookmark_shared_users,
/*History*/
  history_opened,
  history_opened_users,
  history_opened_item,
  history_opened_item_users,
  history_opened_items_in_new_tabs,
  history_opened_items_in_new_tabs_users,
  history_opened_items_in_private_tabs,
  history_opened_items_in_private_tabs_users,
  history_recent_searches_tapped,
  history_recent_searches_tapped_users,
  history_remove_prompt_cancelled,
  history_remove_prompt_cancelled_users,
  history_remove_prompt_opened,
  history_remove_prompt_opened_users,
  history_removed,
  history_removed_users,
  history_removed_all,
  history_removed_all_users,
  history_removed_last_hour,
  history_removed_last_hour_users,
  history_removed_today_and_yesterday,
  history_removed_today_and_yesterday_users,
  history_search_icon_tapped,
  history_search_icon_tapped_users,
  history_search_term_group_open_tab,
  history_search_term_group_open_tab_users,
  history_search_term_group_remove_all,
  history_search_term_group_remove_all_users,
  history_search_term_group_remove_tab,
  history_search_term_group_remove_tab_users,
  history_search_term_group_tapped,
  history_search_term_group_tapped_users,
  history_shared,
  history_shared_users,
/*FxA*/
  sync_failed,
  sync_failed_users,
  sync_account_opened,
  sync_account_opened_users,
  sync_account_send_tab,
  sync_account_send_tab_users,
  sync_account_sign_in_to_send_tab,
  sync_account_sign_in_to_send_tab_users,
  sync_account_sync_now,
  sync_account_sync_now_users,
  sync_auth_closed,
  sync_auth_closed_users,
  sync_auth_opened,
  sync_auth_opened_users,
  sync_auth_other_external,
  sync_auth_other_external_users,
  sync_auth_paired,
  sync_auth_paired_users,
  sync_auth_recovered,
  sync_auth_recovered_users,
  sync_auth_scan_pairing,
  sync_auth_scan_pairing_users,
  sync_auth_sign_in,
  sync_auth_sign_in_users,
  sync_auth_sign_out,
  sync_auth_sign_out_users,
  sync_auth_sign_up,
  sync_auth_sign_up_users,
  sync_auth_use_email,
  sync_auth_use_email_users,
  sync_auth_use_email_problem,
  sync_auth_use_email_problem_users,
/*Privacy*/
  hp_private_mode_tapped,
  hp_private_mode_tapped_users,
  tab_tray_private_mode_switched,
  tab_tray_private_mode_switched_users,
  app_icon_private_tab_tapped,
  app_icon_private_tab_tapped_users,
  tab_tray_private_mode_tapped,
  tab_tray_private_mode_tapped_users,
  etp_setting_changed,
  etp_setting_changed_users,
  etp_settings,
  etp_settings_users,
  etp_shield,
  etp_shield_users,
  etp_tracker_list,
  etp_tracker_list_users,
/*Default Browser*/
  default_browser_changed_users,
  default_browser_changed,
/*Notification*/
  re_engagement_notif_shown,
  re_engagement_notif_shown_users,
  re_engagement_notif_tapped,
  re_engagement_notif_tapped_users,
/*Customize Home*/
  app_menu_customize_homepage,
  app_menu_customize_homepage_users,
  home_page_customize_home_clicked,
  home_page_customize_home_clicked_users
FROM
  events_ping_distinct_client_count
LEFT JOIN
  product_features_agg
USING
  (submission_date)
