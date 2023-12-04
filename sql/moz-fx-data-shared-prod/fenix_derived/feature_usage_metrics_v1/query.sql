-- Query for fenix_derived.feature_usage_metrics_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
DECLARE start_date DATE DEFAULT "2021-05-05";

DECLARE end_date DATE DEFAULT current_date;

WITH dau_segments AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    COUNT(DISTINCT client_info.client_id) AS dau
  FROM
    `mozdata.fenix.metrics`
  WHERE
    DATE(submission_timestamp) >= start_date
  GROUP BY
    1
),
product_features AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    --Credential Management: Logins
    COALESCE(SUM(metrics.counter.logins_deleted), 0) AS logins_deleted,
    COALESCE(SUM(metrics.counter.logins_modified), 0) AS logins_modified,
    COALESCE(SUM(metrics.quantity.logins_saved_all), 0) AS currently_stored_logins,
    --Credential Management: Credit Cards
    COALESCE(SUM(metrics.counter.credit_cards_deleted), 0) AS credit_cards_deleted,
    COALESCE(SUM(metrics.quantity.credit_cards_saved_all), 0) AS currently_stored_credit_cards,
    --Credential Management: Addresses
    COALESCE(SUM(metrics.counter.addresses_deleted), 0) AS addresses_deleted,
    COALESCE(SUM(metrics.counter.addresses_updated), 0) AS addresses_modified,
    COALESCE(SUM(metrics.quantity.addresses_saved_all), 0) AS currently_stored_addresses,
    --Bookmark
    COALESCE(
      SUM(metrics.labeled_counter.metrics_bookmarks_add[SAFE_OFFSET(0)].value),
      0
    ) AS bookmarks_add,
    COALESCE(
      SUM(metrics.labeled_counter.metrics_bookmarks_delete[SAFE_OFFSET(0)].value),
      0
    ) AS bookmarks_delete,
    COALESCE(
      SUM(metrics.labeled_counter.metrics_bookmarks_edit[SAFE_OFFSET(0)].value),
      0
    ) AS bookmarks_edit,
    COALESCE(
      SUM(metrics.labeled_counter.metrics_bookmarks_open[SAFE_OFFSET(0)].value),
      0
    ) AS bookmarks_open,
    COALESCE(
      SUM(metrics.counter.metrics_desktop_bookmarks_count),
      0
    ) AS metrics_desktop_bookmarks_count,
    COALESCE(
      SUM(metrics.counter.metrics_mobile_bookmarks_count),
      0
    ) AS metrics_mobile_bookmarks_count,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.metrics_has_desktop_bookmarks IS TRUE THEN 1 ELSE 0 END),
      0
    ) AS metrics_has_desktop_bookmarks,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.metrics_has_mobile_bookmarks IS TRUE THEN 1 ELSE 0 END),
      0
    ) AS metrics_has_mobile_bookmarks,
    --Privacy
    COALESCE(
      SUM(
        CASE
          WHEN metrics.string.preferences_enhanced_tracking_protection = 'standard'
            THEN 1
          ELSE 0
        END
      ),
      0
    ) AS etp_standard,
    COALESCE(
      SUM(
        CASE
          WHEN metrics.string.preferences_enhanced_tracking_protection = 'strict'
            THEN 1
          ELSE 0
        END
      ),
      0
    ) AS etp_strict,
    COALESCE(
      SUM(
        CASE
          WHEN metrics.string.preferences_enhanced_tracking_protection = 'custom'
            THEN 1
          ELSE 0
        END
      ),
      0
    ) AS etp_custom,
    COALESCE(
      SUM(
        CASE
          WHEN metrics.string.preferences_enhanced_tracking_protection NOT IN (
              'custom',
              'standard',
              'strict'
            )
            THEN 1
          ELSE 0
        END
      ),
      0
    ) AS etp_disabled,
    --Tab count
    COALESCE(
      SUM(metrics.counter.metrics_private_tabs_open_count),
      0
    ) AS metrics_private_tabs_open_count,
    COALESCE(SUM(metrics.counter.metrics_tabs_open_count), 0) AS metrics_tabs_open_count,
    --Default browser
    COALESCE(
      SUM(CASE WHEN metrics.boolean.metrics_default_browser THEN 1 ELSE 0 END)
    ) AS metrics_default_browser,
    --Awesomebar Location
    COALESCE(
      SUM(
        CASE
          WHEN metrics.string.preferences_toolbar_position_setting IN ('top', 'fixed_top')
            THEN 1
          ELSE 0
        END
      ),
      0
    ) AS awesomebar_top,
    COALESCE(
      SUM(
        CASE
          WHEN metrics.string.preferences_toolbar_position_setting = 'bottom'
            THEN 1
          ELSE 0
        END
      ),
      0
    ) AS awesomebar_bottom,
    COALESCE(
      SUM(
        CASE
          WHEN metrics.string.preferences_toolbar_position_setting NOT IN (
              'top',
              'fixed_top',
              'bottom'
            )
            THEN 1
          ELSE 0
        END
      ),
      0
    ) AS awesomebar_null,
    --Notification
    COALESCE(
      SUM(CASE WHEN metrics.boolean.metrics_notifications_allowed THEN 1 ELSE 0 END)
    ) AS metrics_notifications_allowed,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.events_marketing_notification_allowed THEN 1 ELSE 0 END)
    ) AS events_marketing_notification_allowed,
    --Customize Homepage
    COALESCE(
      SUM(CASE WHEN metrics.boolean.customize_home_contile THEN 1 ELSE 0 END)
    ) AS customize_home_contile,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.customize_home_jump_back_in THEN 1 ELSE 0 END)
    ) AS customize_home_jump_back_in,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.customize_home_most_visited_sites THEN 1 ELSE 0 END)
    ) AS customize_home_most_visited_sites,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.customize_home_pocket THEN 1 ELSE 0 END)
    ) AS customize_home_pocket,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.customize_home_recently_saved THEN 1 ELSE 0 END)
    ) AS customize_home_recently_saved,
    COALESCE(
      SUM(CASE WHEN metrics.boolean.customize_home_recently_visited THEN 1 ELSE 0 END)
    ) AS customize_home_recently_visited,
  FROM
    `mozdata.fenix.metrics`
  WHERE
    DATE(submission_timestamp) >= start_date
--AND sample_id = 0
  GROUP BY
    1,
    2
),
product_features_agg AS (
  SELECT
    submission_date,
    /*Logins*/
    --logins deleted
    COUNT(DISTINCT CASE WHEN logins_deleted > 0 THEN client_id END) AS logins_deleted_users,
    COALESCE(SUM(logins_deleted), 0) AS logins_deleted,
    --logins modified
    COUNT(DISTINCT CASE WHEN logins_modified > 0 THEN client_id END) AS logins_modified_users,
    COALESCE(SUM(logins_modified), 0) AS logins_modified,
    --logins currently stored
    COUNT(
      DISTINCT
      CASE
        WHEN currently_stored_logins > 0
          THEN client_id
      END
    ) AS currently_stored_logins_users,
    COALESCE(SUM(currently_stored_logins), 0) AS currently_stored_logins,
    /*Credit Card*/
    --credit card deleted
    COUNT(
      DISTINCT
      CASE
        WHEN credit_cards_deleted > 0
          THEN client_id
      END
    ) AS credit_cards_deleted_users,
    COALESCE(SUM(credit_cards_deleted), 0) AS credit_cards_deleted,
    --credit card currently stored
    COUNT(
      DISTINCT
      CASE
        WHEN currently_stored_credit_cards > 0
          THEN client_id
      END
    ) AS currently_stored_credit_cards_users,
    COALESCE(SUM(currently_stored_credit_cards), 0) AS currently_stored_credit_cards,
    /*Address*/
    --address deleted
    COUNT(DISTINCT CASE WHEN addresses_deleted > 0 THEN client_id END) AS addresses_deleted_users,
    COALESCE(SUM(addresses_deleted), 0) AS addresses_deleted,
    --address modified
    COUNT(DISTINCT CASE WHEN addresses_modified > 0 THEN client_id END) AS addresses_modified_users,
    COALESCE(SUM(addresses_modified), 0) AS addresses_modified,
    -- addresses currently stored
    COUNT(
      DISTINCT
      CASE
        WHEN currently_stored_addresses > 0
          THEN client_id
      END
    ) AS currently_stored_addresses_users,
    COALESCE(SUM(currently_stored_addresses), 0) AS currently_stored_addresses,
    /*Bookmark*/
    --bookmarks_add
    COUNT(DISTINCT CASE WHEN bookmarks_add > 0 THEN client_id END) AS bookmarks_add_users,
    COALESCE(SUM(bookmarks_add), 0) AS bookmarks_add,
    --bookmarks_delete
    COUNT(DISTINCT CASE WHEN bookmarks_delete > 0 THEN client_id END) AS bookmarks_delete_users,
    COALESCE(SUM(bookmarks_delete), 0) AS bookmarks_delete,
    --bookmarks_edit
    COUNT(DISTINCT CASE WHEN bookmarks_edit > 0 THEN client_id END) AS bookmarks_edit_users,
    COALESCE(SUM(bookmarks_edit), 0) AS bookmarks_edit,
    --bookmarks_open
    COUNT(DISTINCT CASE WHEN bookmarks_open > 0 THEN client_id END) AS bookmarks_open_users,
    COALESCE(SUM(bookmarks_open), 0) AS bookmarks_open,
    --metrics_desktop_bookmarks_count
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_desktop_bookmarks_count > 0
          THEN client_id
      END
    ) AS metrics_desktop_bookmarks_count_users,
    COALESCE(SUM(metrics_desktop_bookmarks_count), 0) AS metrics_desktop_bookmarks_count,
    --metrics_mobile_bookmarks_count
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_mobile_bookmarks_count > 0
          THEN client_id
      END
    ) AS metrics_mobile_bookmarks_count_users,
    COALESCE(SUM(metrics_mobile_bookmarks_count), 0) AS metrics_mobile_bookmarks_count,
    --metrics_has_desktop_bookmarks
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_has_desktop_bookmarks > 0
          THEN client_id
      END
    ) AS metrics_has_desktop_bookmarks_users,
    COALESCE(SUM(metrics_has_desktop_bookmarks), 0) AS metrics_has_desktop_bookmarks,
    --metrics_has_mobile_bookmarks
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_has_mobile_bookmarks > 0
          THEN client_id
      END
    ) AS metrics_has_mobile_bookmarks_users,
    COALESCE(SUM(metrics_has_mobile_bookmarks), 0) AS metrics_has_mobile_bookmarks,
    /*Privacy*/
    --etp_standard
    COUNT(DISTINCT CASE WHEN etp_standard > 0 THEN client_id END) AS etp_standard_users,
    COALESCE(SUM(etp_standard), 0) AS etp_standard,
    --etp_strict
    COUNT(DISTINCT CASE WHEN etp_strict > 0 THEN client_id END) AS etp_strict_users,
    COALESCE(SUM(etp_strict), 0) AS etp_strict,
    --etp_custom
    COUNT(DISTINCT CASE WHEN etp_custom > 0 THEN client_id END) AS etp_custom_users,
    COALESCE(SUM(etp_custom), 0) AS etp_custom,
    --etp_disabled
    COUNT(DISTINCT CASE WHEN etp_disabled > 0 THEN client_id END) AS etp_disabled_users,
    COALESCE(SUM(etp_disabled), 0) AS etp_disabled,
    /*Tab Count*/
    --metrics_private_tabs_open_count
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_private_tabs_open_count > 0
          THEN client_id
      END
    ) AS metrics_private_tabs_open_count_users,
    COALESCE(SUM(metrics_private_tabs_open_count), 0) AS metrics_private_tabs_open_count,
    --metrics_tabs_open_count
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_tabs_open_count > 0
          THEN client_id
      END
    ) AS metrics_tabs_open_count_users,
    COALESCE(SUM(metrics_tabs_open_count), 0) AS metrics_tabs_open_count,
    /*Default Browser*/
    --metrics_default_browser
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_default_browser > 0
          THEN client_id
      END
    ) AS metrics_default_browser_users,
    COALESCE(SUM(metrics_default_browser), 0) AS metrics_default_browser,
    /*Awesomebar Location*/
    --awesomebar_top
    COUNT(DISTINCT CASE WHEN awesomebar_top > 0 THEN client_id END) AS awesomebar_top_users,
    COALESCE(SUM(awesomebar_top), 0) AS awesomebar_top,
    --awesomebar_bottom
    COUNT(DISTINCT CASE WHEN awesomebar_bottom > 0 THEN client_id END) AS awesomebar_bottom_users,
    COALESCE(SUM(awesomebar_bottom), 0) AS awesomebar_bottom,
    --awesomebar_null
    COUNT(DISTINCT CASE WHEN awesomebar_null > 0 THEN client_id END) AS awesomebar_null_users,
    COALESCE(SUM(awesomebar_null), 0) AS awesomebar_null,
    /*Notificaion*/
    --metrics_notifications_allowed
    COUNT(
      DISTINCT
      CASE
        WHEN metrics_notifications_allowed > 0
          THEN client_id
      END
    ) AS metrics_notifications_allowed_users,
    COALESCE(SUM(metrics_notifications_allowed), 0) AS metrics_notifications_allowed,
    --events_marketing_notification_allowed
    COUNT(
      DISTINCT
      CASE
        WHEN events_marketing_notification_allowed > 0
          THEN client_id
      END
    ) AS events_marketing_notification_allowed_users,
    COALESCE(
      SUM(events_marketing_notification_allowed),
      0
    ) AS events_marketing_notification_allowed,
    /*Customize Homepage*/
    --customize_home_contile
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_contile > 0
          THEN client_id
      END
    ) AS customize_home_contile_users,
    COALESCE(SUM(customize_home_contile), 0) AS customize_home_contile,
    --customize_home_jump_back_in_users
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_jump_back_in > 0
          THEN client_id
      END
    ) AS customize_home_jump_back_in_users,
    COALESCE(SUM(customize_home_jump_back_in), 0) AS customize_home_jump_back_in,
    --customize_home_most_visited_sites_users
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_most_visited_sites > 0
          THEN client_id
      END
    ) AS customize_home_most_visited_sites_users,
    COALESCE(SUM(customize_home_most_visited_sites), 0) AS customize_home_most_visited_sites,
    --customize_home_pocket_users
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_pocket > 0
          THEN client_id
      END
    ) AS customize_home_pocket_users,
    COALESCE(SUM(customize_home_pocket), 0) AS customize_home_pocket,
    --customize_home_recently_saved_users
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_recently_saved > 0
          THEN client_id
      END
    ) AS customize_home_recently_saved_users,
    COALESCE(SUM(customize_home_recently_saved), 0) AS customize_home_recently_saved,
    --customize_home_recently_visited_users
    COUNT(
      DISTINCT
      CASE
        WHEN customize_home_recently_visited > 0
          THEN client_id
      END
    ) AS customize_home_recently_visited_users,
    COALESCE(SUM(customize_home_recently_visited), 0) AS customize_home_recently_visited,
  FROM
    product_features
  WHERE
    submission_date >= start_date
  GROUP BY
    1
)
SELECT
  d.submission_date,
  dau
/*logins*/
  ,
  logins_deleted_users,
  logins_deleted,
  logins_modified_users,
  logins_modified,
  currently_stored_logins_users,
  currently_stored_logins
/*credit card*/
  ,
  credit_cards_deleted_users,
  credit_cards_deleted,
  currently_stored_credit_cards_users,
  currently_stored_credit_cards
/*addresses*/
  ,
  addresses_deleted_users,
  addresses_deleted,
  addresses_modified_users,
  addresses_modified,
  currently_stored_addresses_users,
  currently_stored_addresses
/*bookmark*/
  ,
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
  metrics_has_mobile_bookmarks
/*privacy*/
  ,
  etp_standard_users,
  etp_standard,
  etp_strict_users,
  etp_strict,
  etp_custom_users,
  etp_custom,
  etp_disabled_users,
  etp_disabled
/*Tab count*/
  ,
  metrics_private_tabs_open_count_users,
  metrics_private_tabs_open_count,
  metrics_tabs_open_count_users,
  metrics_tabs_open_count
/*Default browser*/
  ,
  metrics_default_browser_users,
  metrics_default_browser
/*Notification*/
  ,
  metrics_notifications_allowed_users,
  metrics_notifications_allowed,
  events_marketing_notification_allowed_users,
  events_marketing_notification_allowed
/*Customize Home*/
  ,
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
  customize_home_recently_visited
FROM
  dau_segments d
LEFT JOIN
  product_features_agg p
ON
  d.submission_date = p.submission_date
