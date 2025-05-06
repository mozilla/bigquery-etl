WITH baseline_clients AS (
  SELECT
    DATE(
      DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')
    ) AS ping_date,
    client_info.client_id,
    normalized_channel AS channel,
    normalized_country_code AS country,
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 4 DAY)
    AND @submission_date
    AND DATE(
      DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')
    ) = DATE_SUB(@submission_date, INTERVAL 4 DAY)
    AND metrics.timespan.glean_baseline_duration.value > 0
    AND LOWER(metadata.isp.name) <> "browserstack"
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
    `moz-fx-data-shared-prod.fenix.attribution_clients`
),
metric_ping_clients_feature_usage AS (
  SELECT
    -- In rare cases we can have an end_time that is earlier than the start_time, we made the decision
    -- to attribute the metrics to the earlier date of the two.
    DATE(
      DATETIME(LEAST(ping_info.parsed_start_time, ping_info.parsed_end_time), 'UTC')
    ) AS ping_date,
    client_info.client_id,
    normalized_channel AS channel,
    normalized_country_code AS country,
    COALESCE(metrics.boolean.metrics_default_browser, FALSE) AS is_default_browser,
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
    COALESCE(SUM(metrics_bookmarks_add_table.value), 0) AS bookmarks_add,
    COALESCE(SUM(metrics_bookmarks_delete_table.value), 0) AS bookmarks_delete,
    COALESCE(SUM(metrics_bookmarks_edit_table.value), 0) AS bookmarks_edit,
    COALESCE(SUM(metrics_bookmarks_open_table.value), 0) AS bookmarks_open,
    COALESCE(
      SUM(metrics.counter.metrics_desktop_bookmarks_count),
      0
    ) AS metrics_desktop_bookmarks_count,
    COALESCE(
      SUM(metrics.counter.metrics_mobile_bookmarks_count),
      0
    ) AS metrics_mobile_bookmarks_count,
    COUNTIF(metrics.boolean.metrics_has_desktop_bookmarks) AS metrics_has_desktop_bookmarks,
    COUNTIF(metrics.boolean.metrics_has_mobile_bookmarks) AS metrics_has_mobile_bookmarks,
    --Privacy
    COUNTIF(
      LOWER(metrics.string.preferences_enhanced_tracking_protection) = "standard"
    ) AS etp_standard,
    COUNTIF(
      LOWER(metrics.string.preferences_enhanced_tracking_protection) = "strict"
    ) AS etp_strict,
    COUNTIF(
      LOWER(metrics.string.preferences_enhanced_tracking_protection) = "custom"
    ) AS etp_custom,
    COUNTIF(
      LOWER(metrics.string.preferences_enhanced_tracking_protection) NOT IN (
        'custom',
        'standard',
        'strict'
      )
    ) AS etp_disabled,
    --Tab count
    COALESCE(
      SUM(metrics.counter.metrics_private_tabs_open_count),
      0
    ) AS metrics_private_tabs_open_count,
    COALESCE(SUM(metrics.counter.metrics_tabs_open_count), 0) AS metrics_tabs_open_count,
    --Default browser
    COUNTIF(metrics.boolean.metrics_default_browser) AS metrics_default_browser,
    --Awesomebar Location
    COUNTIF(
      LOWER(metrics.string.preferences_toolbar_position_setting) IN ('top', 'fixed_top')
    ) AS awesomebar_top,
    COUNTIF(
      LOWER(metrics.string.preferences_toolbar_position_setting) = 'bottom'
    ) AS awesomebar_bottom,
    COUNTIF(
      LOWER(metrics.string.preferences_toolbar_position_setting) NOT IN (
        'top',
        'fixed_top',
        'bottom'
      )
    ) AS awesomebar_null,
    --Notification
    COUNTIF(metrics.boolean.metrics_notifications_allowed) AS metrics_notifications_allowed,
    COUNTIF(
      metrics.boolean.events_marketing_notification_allowed
    ) AS events_marketing_notification_allowed,
    --Customize Homepage
    COUNTIF(metrics.boolean.customize_home_contile) AS customize_home_contile,
    COUNTIF(metrics.boolean.customize_home_jump_back_in) AS customize_home_jump_back_in,
    COUNTIF(metrics.boolean.customize_home_most_visited_sites) AS customize_home_most_visited_sites,
    COUNTIF(metrics.boolean.customize_home_pocket) AS customize_home_pocket,
    COUNTIF(metrics.boolean.customize_home_recently_saved) AS customize_home_recently_saved,
    COUNTIF(metrics.boolean.customize_home_recently_visited) AS customize_home_recently_visited
  FROM
    `moz-fx-data-shared-prod.fenix.metrics` AS metric
  LEFT JOIN
    UNNEST(metrics.labeled_counter.metrics_bookmarks_add) AS metrics_bookmarks_add_table
  LEFT JOIN
    UNNEST(metrics.labeled_counter.metrics_bookmarks_delete) AS metrics_bookmarks_delete_table
  LEFT JOIN
    UNNEST(metrics.labeled_counter.metrics_bookmarks_edit) AS metrics_bookmarks_edit_table
  LEFT JOIN
    UNNEST(metrics.labeled_counter.metrics_bookmarks_open) AS metrics_bookmarks_open_table
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
    country,
    is_default_browser
)
-- Aggregated feature usage
SELECT
  @submission_date AS submission_date,
  ping_date,
  channel,
  country,
  adjust_network,
  is_default_browser,
  /*Logins*/
  --logins deleted
  COUNT(DISTINCT IF(logins_deleted > 0, client_id, NULL)) AS logins_deleted_users,
  SUM(logins_deleted) AS logins_deleted,
  --logins modified
  COUNT(DISTINCT IF(logins_modified > 0, client_id, NULL)) AS logins_modified_users,
  SUM(logins_modified) AS logins_modified,
  --logins currently stored
  COUNT(DISTINCT IF(currently_stored_logins > 0, client_id, NULL)) AS currently_stored_logins_users,
  SUM(currently_stored_logins) AS currently_stored_logins,
  /*Credit Card*/
  --credit card deleted
  COUNT(DISTINCT IF(credit_cards_deleted > 0, client_id, NULL)) AS credit_cards_deleted_users,
  SUM(credit_cards_deleted) AS credit_cards_deleted,
  --credit card currently stored
  COUNT(
    DISTINCT IF(currently_stored_credit_cards > 0, client_id, NULL)
  ) AS currently_stored_credit_cards_users,
  SUM(currently_stored_credit_cards) AS currently_stored_credit_cards,
  /*Address*/
  --address deleted
  COUNT(DISTINCT IF(addresses_deleted > 0, client_id, NULL)) AS addresses_deleted_users,
  SUM(addresses_deleted) AS addresses_deleted,
  --address modified
  COUNT(DISTINCT IF(addresses_modified > 0, client_id, NULL)) AS addresses_modified_users,
  SUM(addresses_modified) AS addresses_modified,
  -- addresses currently stored
  COUNT(
    DISTINCT IF(currently_stored_addresses > 0, client_id, NULL)
  ) AS currently_stored_addresses_users,
  SUM(currently_stored_addresses) AS currently_stored_addresses,
  /*Bookmark*/
  --bookmarks_add
  COUNT(DISTINCT IF(bookmarks_add > 0, client_id, NULL)) AS bookmarks_add_users,
  SUM(bookmarks_add) AS bookmarks_add,
  --bookmarks_delete
  COUNT(DISTINCT IF(bookmarks_delete > 0, client_id, NULL)) AS bookmarks_delete_users,
  SUM(bookmarks_delete) AS bookmarks_delete,
  --bookmarks_edit
  COUNT(DISTINCT IF(bookmarks_edit > 0, client_id, NULL)) AS bookmarks_edit_users,
  SUM(bookmarks_edit) AS bookmarks_edit,
  --bookmarks_open
  COUNT(DISTINCT IF(bookmarks_open > 0, client_id, NULL)) AS bookmarks_open_users,
  SUM(bookmarks_open) AS bookmarks_open,
  --metrics_desktop_bookmarks_count
  COUNT(
    DISTINCT IF(metrics_desktop_bookmarks_count > 0, client_id, NULL)
  ) AS metrics_desktop_bookmarks_count_users,
  SUM(metrics_desktop_bookmarks_count) AS metrics_desktop_bookmarks_count,
  --metrics_mobile_bookmarks_count
  COUNT(
    DISTINCT IF(metrics_mobile_bookmarks_count > 0, client_id, NULL)
  ) AS metrics_mobile_bookmarks_count_users,
  SUM(metrics_mobile_bookmarks_count) AS metrics_mobile_bookmarks_count,
  --metrics_has_desktop_bookmarks
  COUNT(
    DISTINCT IF(metrics_has_desktop_bookmarks > 0, client_id, NULL)
  ) AS metrics_has_desktop_bookmarks_users,
  SUM(metrics_has_desktop_bookmarks) AS metrics_has_desktop_bookmarks,
  --metrics_has_mobile_bookmarks
  COUNT(
    DISTINCT IF(metrics_has_mobile_bookmarks > 0, client_id, NULL)
  ) AS metrics_has_mobile_bookmarks_users,
  SUM(metrics_has_mobile_bookmarks) AS metrics_has_mobile_bookmarks,
  /*Privacy*/
  --etp_standard
  COUNT(DISTINCT IF(etp_standard > 0, client_id, NULL)) AS etp_standard_users,
  SUM(etp_standard) AS etp_standard,
  --etp_strict
  COUNT(DISTINCT IF(etp_strict > 0, client_id, NULL)) AS etp_strict_users,
  SUM(etp_strict) AS etp_strict,
  --etp_custom
  COUNT(DISTINCT IF(etp_custom > 0, client_id, NULL)) AS etp_custom_users,
  SUM(etp_custom) AS etp_custom,
    --etp_disabled
  COUNT(DISTINCT IF(etp_disabled > 0, client_id, NULL)) AS etp_disabled_users,
  SUM(etp_disabled) AS etp_disabled,
  /*Tab Count*/
  --metrics_private_tabs_open_count
  COUNT(
    DISTINCT IF(metrics_private_tabs_open_count > 0, client_id, NULL)
  ) AS metrics_private_tabs_open_count_users,
  SUM(metrics_private_tabs_open_count) AS metrics_private_tabs_open_count,
  --metrics_tabs_open_count
  COUNT(DISTINCT IF(metrics_tabs_open_count > 0, client_id, NULL)) AS metrics_tabs_open_count_users,
  SUM(metrics_tabs_open_count) AS metrics_tabs_open_count,
  /*Default Browser*/
  --metrics_default_browser
  COUNT(DISTINCT IF(metrics_default_browser > 0, client_id, NULL)) AS metrics_default_browser_users,
  SUM(metrics_default_browser) AS metrics_default_browser,
  /*Awesomebar Location*/
  --awesomebar_top
  COUNT(DISTINCT IF(awesomebar_top > 0, client_id, NULL)) AS awesomebar_top_users,
  --awesomebar_bottom
  COUNT(DISTINCT IF(awesomebar_bottom > 0, client_id, NULL)) AS awesomebar_bottom_users,
  --awesomebar_null
  COUNT(DISTINCT IF(awesomebar_null > 0, client_id, NULL)) AS awesomebar_null_users,
  /*Notificaion*/
  --metrics_notifications_allowed
  COUNT(
    DISTINCT IF(metrics_notifications_allowed > 0, client_id, NULL)
  ) AS metrics_notifications_allowed_users,
  SUM(metrics_notifications_allowed) AS metrics_notifications_allowed,
  --events_marketing_notification_allowed
  COUNT(
    DISTINCT IF(events_marketing_notification_allowed > 0, client_id, NULL)
  ) AS events_marketing_notification_allowed_users,
  SUM(events_marketing_notification_allowed) AS events_marketing_notification_allowed,
  /*Customize Homepage*/
  --customize_home_contile
  COUNT(DISTINCT IF(customize_home_contile > 0, client_id, NULL)) AS customize_home_contile_users,
  SUM(customize_home_contile) AS customize_home_contile,
  --customize_home_jump_back_in_users
  COUNT(
    DISTINCT IF(customize_home_jump_back_in > 0, client_id, NULL)
  ) AS customize_home_jump_back_in_users,
  SUM(customize_home_jump_back_in) AS customize_home_jump_back_in,
  --customize_home_most_visited_sites_users
  COUNT(
    DISTINCT IF(customize_home_most_visited_sites > 0, client_id, NULL)
  ) AS customize_home_most_visited_sites_users,
  SUM(customize_home_most_visited_sites) AS customize_home_most_visited_sites,
  --customize_home_pocket_users
  COUNT(DISTINCT IF(customize_home_pocket > 0, client_id, NULL)) AS customize_home_pocket_users,
  SUM(customize_home_pocket) AS customize_home_pocket,
  --customize_home_recently_saved_users
  COUNT(
    DISTINCT IF(customize_home_recently_saved > 0, client_id, NULL)
  ) AS customize_home_recently_saved_users,
  SUM(customize_home_recently_saved) AS customize_home_recently_saved,
  --customize_home_recently_visited_users
  COUNT(
    DISTINCT IF(customize_home_recently_visited > 0, client_id, NULL)
  ) AS customize_home_recently_visited_users,
  SUM(customize_home_recently_visited) AS customize_home_recently_visited
FROM
  metric_ping_clients_feature_usage
INNER JOIN
  baseline_clients
  USING (ping_date, client_id, channel, country)
LEFT JOIN
  client_attribution
  USING (client_id, channel)
GROUP BY
  submission_date,
  ping_date,
  channel,
  country,
  adjust_network,
  is_default_browser
