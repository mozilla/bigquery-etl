WITH dau AS 
(SELECT DATE(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time)) AS baseline_ping_date,
client_info.client_id
FROM fenix.baseline
WHERE DATE(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time))= @ping_date
AND metrics.timespan.glean_baseline_duration.value > 0
AND LOWER(COALESCE(metadata.isp.name, "")) <> "browserstack"
AND DATE(submission_timestamp) BETWEEN @ping_date AND DATE_ADD(@ping_date, INTERVAL 4 DAY)
),

metrics_client AS
(SELECT DATE(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time)) AS metrics_ping_date,
client_info.client_id
FROM fenix.metrics
WHERE DATE(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time))= @ping_date
AND DATE(submission_timestamp) BETWEEN @ping_date AND DATE_ADD(@ping_date, INTERVAL 4 DAY)
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
DATE(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time)) AS metrics_ping_date,
--Credential Management: Logins
metrics.counter.logins_deleted,
metrics.counter.logins_modified,
metrics.quantity.logins_saved_all,
--Credential Management: Credit Cards
metrics.counter.credit_cards_deleted,
metrics.quantity.credit_cards_saved_all,
--Credential Management: Addresses
metrics.counter.addresses_deleted,
metrics.counter.addresses_updated,
metrics.quantity.addresses_saved_all,
--Bookmark
metrics_bookmarks_add_table.value AS metrics_bookmarks_add_value,
metrics_bookmarks_delete_table.value AS metrics_bookmarks_delete_value,
metrics_bookmarks_edit_table.value AS metrics_bookmarks_edit_value,
metrics_bookmarks_open_table.value AS metrics_bookmarks_open_value,
metrics.counter.metrics_desktop_bookmarks_count,
metrics.counter.metrics_mobile_bookmarks_count,
metrics.boolean.metrics_has_desktop_bookmarks,
metrics.boolean.metrics_has_mobile_bookmarks,
--Privacy
metrics.string.preferences_enhanced_tracking_protection,
--Tab Count
metrics.counter.metrics_private_tabs_open_count,
metrics.counter.metrics_tabs_open_count,
--Default Browser
metrics.boolean.metrics_default_browser,
--Awesomebar Location
metrics.string.preferences_toolbar_position_setting,
--Notification
metrics.boolean.metrics_notifications_allowed,
metrics.boolean.events_marketing_notification_allowed,
--Customize Homepage
metrics.boolean.customize_home_contile,
metrics.boolean.customize_home_jump_back_in,
metrics.boolean.customize_home_most_visited_sites,
metrics.boolean.customize_home_pocket,
metrics.boolean.customize_home_recently_saved,
metrics.boolean.customize_home_recently_visited
FROM fenix.metrics AS metric
LEFT JOIN UNNEST(metrics.labeled_counter.metrics_bookmarks_add) AS metrics_bookmarks_add_table
LEFT JOIN UNNEST(metrics.labeled_counter.metrics_bookmarks_delete) AS metrics_bookmarks_delete_table
LEFT JOIN UNNEST(metrics.labeled_counter.metrics_bookmarks_edit) AS metrics_bookmarks_edit_table
LEFT JOIN UNNEST(metrics.labeled_counter.metrics_bookmarks_open) AS metrics_bookmarks_open_table
WHERE DATE(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time))= @ping_date
AND DATE(submission_timestamp) BETWEEN @ping_date AND DATE_ADD(@ping_date, INTERVAL 4 DAY)) metrics
JOIN metrics_dau
ON DATE(metrics.metrics_ping_date) = metrics_dau.metrics_ping_date
AND metrics.client_id = metrics_dau.client_id),


client_product_feature_usage AS (
  SELECT
    client_id,
    metrics_ping_date,
    --Credential Management: Logins
    COALESCE(SUM(logins_deleted), 0) AS logins_deleted,
    COALESCE(SUM(logins_modified), 0) AS logins_modified,
    COALESCE(SUM(logins_saved_all), 0) AS currently_stored_logins,
    --Credential Management: Credit Cards
    COALESCE(SUM(credit_cards_deleted), 0) AS credit_cards_deleted,
    COALESCE(SUM(credit_cards_saved_all), 0) AS currently_stored_credit_cards,
    --Credential Management: Addresses
    COALESCE(SUM(addresses_deleted), 0) AS addresses_deleted,
    COALESCE(SUM(addresses_updated), 0) AS addresses_modified,
    COALESCE(SUM(addresses_saved_all), 0) AS currently_stored_addresses,
    --Bookmark
    COALESCE(
      SUM(metrics_bookmarks_add_value),
      0
    ) AS bookmarks_add,
    COALESCE(
      SUM(metrics_bookmarks_delete_value),
      0
    ) AS bookmarks_delete,
    COALESCE(
      SUM(metrics_bookmarks_edit_value),
      0
    ) AS bookmarks_edit,
    COALESCE(
      SUM(metrics_bookmarks_open_value),
      0
    ) AS bookmarks_open,
    COALESCE(
      SUM(metrics_desktop_bookmarks_count),
      0
    ) AS metrics_desktop_bookmarks_count,
    COALESCE(
      SUM(metrics_mobile_bookmarks_count),
      0
    ) AS metrics_mobile_bookmarks_count,
    SUM(CASE WHEN metrics_has_desktop_bookmarks IS TRUE THEN 1 ELSE 0 END) AS metrics_has_desktop_bookmarks,
    SUM(CASE WHEN metrics_has_mobile_bookmarks IS TRUE THEN 1 ELSE 0 END) AS metrics_has_mobile_bookmarks,
    --Privacy
    SUM(
        CASE
          WHEN preferences_enhanced_tracking_protection = 'standard'
            THEN 1
          ELSE 0
        END
      ) AS etp_standard,
    SUM(
        CASE
          WHEN preferences_enhanced_tracking_protection = 'strict'
            THEN 1
          ELSE 0
        END
      ) AS etp_strict,
    SUM(
        CASE
          WHEN preferences_enhanced_tracking_protection = 'custom'
            THEN 1
          ELSE 0
        END
      ) AS etp_custom,
    SUM(
        CASE
          WHEN preferences_enhanced_tracking_protection NOT IN (
              'custom',
              'standard',
              'strict'
            )
            THEN 1
          ELSE 0
        END
      ) AS etp_disabled,
    --Tab count
    COALESCE(
      SUM(metrics_private_tabs_open_count),
      0
    ) AS metrics_private_tabs_open_count,
    COALESCE(SUM(metrics_tabs_open_count), 0) AS metrics_tabs_open_count,
    --Default browser
    SUM(CASE WHEN metrics_default_browser THEN 1 ELSE 0 END) AS metrics_default_browser,
    --Awesomebar Location
    SUM(
        CASE
          WHEN preferences_toolbar_position_setting IN ('top', 'fixed_top')
            THEN 1
          ELSE 0
        END
      ) AS awesomebar_top,
    SUM(
        CASE
          WHEN preferences_toolbar_position_setting = 'bottom'
            THEN 1
          ELSE 0
        END
      ) AS awesomebar_bottom,
    SUM(
        CASE
          WHEN preferences_toolbar_position_setting NOT IN (
              'top',
              'fixed_top',
              'bottom'
            )
            THEN 1
          ELSE 0
        END
      ) AS awesomebar_null,
    --Notification
    SUM(CASE WHEN metrics_notifications_allowed THEN 1 ELSE 0 END) AS metrics_notifications_allowed,
    SUM(CASE WHEN events_marketing_notification_allowed THEN 1 ELSE 0 END) AS events_marketing_notification_allowed,
    --Customize Homepage
    SUM(CASE WHEN customize_home_contile THEN 1 ELSE 0 END) AS customize_home_contile,
    SUM(CASE WHEN customize_home_jump_back_in THEN 1 ELSE 0 END) AS customize_home_jump_back_in,
    SUM(CASE WHEN customize_home_most_visited_sites THEN 1 ELSE 0 END) AS customize_home_most_visited_sites,
    SUM(CASE WHEN customize_home_pocket THEN 1 ELSE 0 END) AS customize_home_pocket,
    SUM(CASE WHEN customize_home_recently_saved THEN 1 ELSE 0 END) AS customize_home_recently_saved,
    SUM(CASE WHEN customize_home_recently_visited THEN 1 ELSE 0 END) AS customize_home_recently_visited
  FROM
    metrics_dau_pings AS metric
  WHERE
    metrics_ping_date = @ping_date
  GROUP BY
    client_id,
    metrics_ping_date
),

product_features_agg AS (
  SELECT
    metrics_ping_date,
    /*Logins*/
    --logins deleted
    COUNT(DISTINCT CASE WHEN logins_deleted > 0 THEN client_id END) AS logins_deleted_users,
    SUM(logins_deleted) AS logins_deleted,
    --logins modified
    COUNT(DISTINCT CASE WHEN logins_modified > 0 THEN client_id END) AS logins_modified_users,
    SUM(logins_modified) AS logins_modified,
    --logins currently stored
    COUNT(
      DISTINCT
      CASE
        WHEN currently_stored_logins > 0
          THEN client_id
      END
    ) AS currently_stored_logins_users,
    SUM(currently_stored_logins) AS currently_stored_logins,
    /*Credit Card*/
    --credit card deleted
    COUNT(
      DISTINCT
      CASE
        WHEN credit_cards_deleted > 0
          THEN client_id
      END
    ) AS credit_cards_deleted_users,
    SUM(credit_cards_deleted) AS credit_cards_deleted,
    --credit card currently stored
    COUNT(
      DISTINCT
      CASE
        WHEN currently_stored_credit_cards > 0
          THEN client_id
      END
    ) AS currently_stored_credit_cards_users,
    SUM(currently_stored_credit_cards) AS currently_stored_credit_cards,
    /*Address*/
    --address deleted
    COUNT(DISTINCT CASE WHEN addresses_deleted > 0 THEN client_id END) AS addresses_deleted_users,
    SUM(addresses_deleted) AS addresses_deleted,
    --address modified
    COUNT(DISTINCT CASE WHEN addresses_modified > 0 THEN client_id END) AS addresses_modified_users,
    SUM(addresses_modified) AS addresses_modified,
    -- addresses currently stored
    COUNT(
      DISTINCT
      CASE
        WHEN currently_stored_addresses > 0
          THEN client_id
      END
    ) AS currently_stored_addresses_users,
    SUM(currently_stored_addresses) AS currently_stored_addresses,
    /*Bookmark*/
    --bookmarks_add
    COUNT(DISTINCT CASE WHEN bookmarks_add > 0 THEN client_id END) AS bookmarks_add_users,
    SUM(bookmarks_add)AS bookmarks_add,
    --bookmarks_delete
    COUNT(DISTINCT CASE WHEN bookmarks_delete > 0 THEN client_id END) AS bookmarks_delete_users,
    SUM(bookmarks_delete) AS bookmarks_delete,
    --bookmarks_edit
    COUNT(DISTINCT CASE WHEN bookmarks_edit > 0 THEN client_id END) AS bookmarks_edit_users,
    SUM(bookmarks_edit) AS bookmarks_edit,
    --bookmarks_open
    COUNT(DISTINCT CASE WHEN bookmarks_open > 0 THEN client_id END) AS bookmarks_open_users,
    SUM(bookmarks_open) AS bookmarks_open,
    --metrics_desktop_bookmarks_count
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_desktop_bookmarks_count > 0
          THEN client_id
      END
    ) AS metrics_desktop_bookmarks_count_users,
    SUM(metrics_desktop_bookmarks_count) AS metrics_desktop_bookmarks_count,
    --metrics_mobile_bookmarks_count
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_mobile_bookmarks_count > 0
          THEN client_id
      END
    ) AS metrics_mobile_bookmarks_count_users,
    SUM(metrics_mobile_bookmarks_count) AS metrics_mobile_bookmarks_count,
    --metrics_has_desktop_bookmarks
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_has_desktop_bookmarks > 0
          THEN client_id
      END
    ) AS metrics_has_desktop_bookmarks_users,
    SUM(metrics_has_desktop_bookmarks) AS metrics_has_desktop_bookmarks,
    --metrics_has_mobile_bookmarks
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_has_mobile_bookmarks > 0
          THEN client_id
      END
    ) AS metrics_has_mobile_bookmarks_users,
    SUM(metrics_has_mobile_bookmarks) AS metrics_has_mobile_bookmarks,
    /*Privacy*/
    --etp_standard
    COUNT(DISTINCT CASE WHEN etp_standard > 0 THEN client_id END) AS etp_standard_users,
    SUM(etp_standard) AS etp_standard,
    --etp_strict
    COUNT(DISTINCT CASE WHEN etp_strict > 0 THEN client_id END) AS etp_strict_users,
    SUM(etp_strict) AS etp_strict,
    --etp_custom
    COUNT(DISTINCT CASE WHEN etp_custom > 0 THEN client_id END) AS etp_custom_users,
    SUM(etp_custom) AS etp_custom,
    --etp_disabled
    COUNT(DISTINCT CASE WHEN etp_disabled > 0 THEN client_id END) AS etp_disabled_users,
    SUM(etp_disabled) AS etp_disabled,
    /*Tab Count*/
    --metrics_private_tabs_open_count
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_private_tabs_open_count > 0
          THEN client_id
      END
    ) AS metrics_private_tabs_open_count_users,
    SUM(metrics_private_tabs_open_count) AS metrics_private_tabs_open_count,
    --metrics_tabs_open_count
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_tabs_open_count > 0
          THEN client_id
      END
    ) AS metrics_tabs_open_count_users,
    SUM(metrics_tabs_open_count) AS metrics_tabs_open_count,
    /*Default Browser*/
    --metrics_default_browser
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_default_browser > 0
          THEN client_id
      END
    ) AS metrics_default_browser_users,
    SUM(metrics_default_browser) AS metrics_default_browser,
    /*Awesomebar Location*/
    --awesomebar_top
    COUNT(DISTINCT CASE WHEN awesomebar_top > 0 THEN client_id END) AS awesomebar_top_users,
    --awesomebar_bottom
    COUNT(DISTINCT CASE WHEN awesomebar_bottom > 0 THEN client_id END) AS awesomebar_bottom_users,
    --awesomebar_null
    COUNT(DISTINCT CASE WHEN awesomebar_null > 0 THEN client_id END) AS awesomebar_null_users,
    /*Notificaion*/
    --metrics_notifications_allowed
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_notifications_allowed > 0
          THEN client_id
      END
    ) AS metrics_notifications_allowed_users,
    SUM(metrics_notifications_allowed) AS metrics_notifications_allowed,
    --events_marketing_notification_allowed
    COUNT(
      DISTINCT
      CASE
        WHEN events_marketing_notification_allowed > 0
          THEN client_id
      END
    ) AS events_marketing_notification_allowed_users,
    SUM(events_marketing_notification_allowed) AS events_marketing_notification_allowed,
    /*Customize Homepage*/
    --customize_home_contile
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_contile > 0
          THEN client_id
      END
    ) AS customize_home_contile_users,
    SUM(customize_home_contile) AS customize_home_contile,
    --customize_home_jump_back_in_users
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_jump_back_in > 0
          THEN client_id
      END
    ) AS customize_home_jump_back_in_users,
    SUM(customize_home_jump_back_in) AS customize_home_jump_back_in,
    --customize_home_most_visited_sites_users
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_most_visited_sites > 0
          THEN client_id
      END
    ) AS customize_home_most_visited_sites_users,
    SUM(customize_home_most_visited_sites) AS customize_home_most_visited_sites,
    --customize_home_pocket_users
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_pocket > 0
          THEN client_id
      END
    ) AS customize_home_pocket_users,
    SUM(customize_home_pocket) AS customize_home_pocket,
    --customize_home_recently_saved_users
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_recently_saved > 0
          THEN client_id
      END
    ) AS customize_home_recently_saved_users,
    SUM(customize_home_recently_saved) AS customize_home_recently_saved,
    --customize_home_recently_visited_users
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_recently_visited > 0
          THEN client_id
      END
    ) AS customize_home_recently_visited_users,
    SUM(customize_home_recently_visited) AS customize_home_recently_visited,
  FROM
    client_product_feature_usage
  GROUP BY
    metrics_ping_date
)
SELECT
  metrics_ping_date,
/*logins*/
  logins_deleted_users,
  logins_deleted,
  logins_modified_users,
  logins_modified,
  currently_stored_logins_users,
  currently_stored_logins,
/*credit card*/
  credit_cards_deleted_users,
  credit_cards_deleted,
  currently_stored_credit_cards_users,
  currently_stored_credit_cards,
/*addresses*/
  addresses_deleted_users,
  addresses_deleted,
  addresses_modified_users,
  addresses_modified,
  currently_stored_addresses_users,
  currently_stored_addresses,
/*bookmark*/
  bookmarks_add_users,
  bookmarks_add,
  bookmarks_delete_users,
  bookmarks_delete,
  bookmarks_edit_users,
  bookmarks_edit,
  bookmarks_open_users,
  bookmarks_open,
  metrics_desktop_bookmarks_count_users,
  metrics_desktop_bookmarks_count,
  metrics_mobile_bookmarks_count_users,
  metrics_mobile_bookmarks_count,
  metrics_has_desktop_bookmarks_users,
  metrics_has_desktop_bookmarks,
  metrics_has_mobile_bookmarks_users,
  metrics_has_mobile_bookmarks,
/*privacy*/
  etp_standard_users,
  etp_standard,
  etp_strict_users,
  etp_strict,
  etp_custom_users,
  etp_custom,
  etp_disabled_users,
  etp_disabled,
/*Tab count*/
  metrics_private_tabs_open_count_users,
  metrics_private_tabs_open_count,
  metrics_tabs_open_count_users,
  metrics_tabs_open_count,
/*Default browser*/
  metrics_default_browser_users,
  metrics_default_browser,
/*Notification*/
  metrics_notifications_allowed_users,
  metrics_notifications_allowed,
  events_marketing_notification_allowed_users,
  events_marketing_notification_allowed,
/*Customize Home*/
  customize_home_contile_users,
  customize_home_contile,
  customize_home_jump_back_in_users,
  customize_home_jump_back_in,
  customize_home_most_visited_sites_users,
  customize_home_most_visited_sites,
  customize_home_pocket_users,
  customize_home_pocket,
  customize_home_recently_saved_users,
  customize_home_recently_saved,
  customize_home_recently_visited_users,
  customize_home_recently_visited,
/*Awesomebar*/
  awesomebar_top_users,
  awesomebar_bottom_users,
  awesomebar_null_users
FROM
  product_features_agg
