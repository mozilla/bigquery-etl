WITH baseline_clients AS (
  SELECT
    submission_date AS dau_date,
    client_id,
    LEAD(submission_date) OVER (PARTITION BY client_id ORDER BY submission_date) AS next_dau
  FROM
    `moz-fx-data-shared-prod.fenix.baseline_clients_daily`
  WHERE
    submission_date >= DATE_SUB(@submission_date, INTERVAL 4 DAY)
    AND durations > 0
    AND LOWER(COALESCE(isp, "")) <> "browserstack"
    AND LOWER(COALESCE(distribution_id, "")) <> "mozillaonline"
),
client_attribution AS (
  SELECT
    client_id,
    adjust_network,
  FROM
    `moz-fx-data-shared-prod.fenix.attribution_clients`
),
metrics_dau AS (
  -- assign a DAU date for each metric ping while keeping it de-duplicated
  SELECT
    m.*,
    MAX(dau_date) OVER (PARTITION BY document_id) AS dau_date
  FROM
    `moz-fx-data-shared-prod.fenix.metrics` m
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
    LOGICAL_OR(COALESCE(metrics.boolean.metrics_default_browser, FALSE)) AS is_default_browser,
    --Credential Management: Logins
    SUM(COALESCE(metrics.counter.logins_deleted, 0)) AS logins_deleted,
    SUM(COALESCE(metrics.counter.logins_modified, 0)) AS logins_modified,
    MAX(COALESCE(metrics.quantity.logins_saved_all, 0)) AS currently_stored_logins,
    --Credential Management: Credit Cards
    SUM(COALESCE(metrics.counter.credit_cards_deleted, 0)) AS credit_cards_deleted,
    MAX(COALESCE(metrics.quantity.credit_cards_saved_all, 0)) AS currently_stored_credit_cards,
    --Credential Management: Addresses
    SUM(COALESCE(metrics.counter.addresses_deleted, 0)) AS addresses_deleted,
    SUM(COALESCE(metrics.counter.addresses_updated, 0)) AS addresses_modified,
    MAX(COALESCE(metrics.quantity.addresses_saved_all, 0)) AS currently_stored_addresses,
    --Bookmark
    SUM(
      COALESCE(
        mozfun.map.extract_keyed_scalar_sum(metrics.labeled_counter.metrics_bookmarks_add),
        0
      )
    ) AS bookmarks_add,
    SUM(
      COALESCE(
        mozfun.map.extract_keyed_scalar_sum(metrics.labeled_counter.metrics_bookmarks_delete),
        0
      )
    ) AS bookmarks_delete,
    SUM(
      COALESCE(
        mozfun.map.extract_keyed_scalar_sum(metrics.labeled_counter.metrics_bookmarks_edit),
        0
      )
    ) AS bookmarks_edit,
    SUM(
      COALESCE(
        mozfun.map.extract_keyed_scalar_sum(metrics.labeled_counter.metrics_bookmarks_open),
        0
      )
    ) AS bookmarks_open,
    SUM(
      COALESCE(metrics.counter.metrics_desktop_bookmarks_count, 0)
    ) AS metrics_desktop_bookmarks_count,
    SUM(
      COALESCE(metrics.counter.metrics_mobile_bookmarks_count, 0)
    ) AS metrics_mobile_bookmarks_count,
    CAST(
      MAX(COALESCE(metrics.boolean.metrics_has_desktop_bookmarks, FALSE)) AS INT64
    ) AS metrics_has_desktop_bookmarks,
    CAST(
      MAX(COALESCE(metrics.boolean.metrics_has_mobile_bookmarks, FALSE)) AS INT64
    ) AS metrics_has_mobile_bookmarks,
    --Privacy
    CAST(
      MAX(
        COALESCE(LOWER(metrics.string.preferences_enhanced_tracking_protection) = "standard", FALSE)
      ) AS INT64
    ) AS etp_standard,
    CAST(
      MAX(
        COALESCE(LOWER(metrics.string.preferences_enhanced_tracking_protection) = "strict", FALSE)
      ) AS INT64
    ) AS etp_strict,
    CAST(
      MAX(
        COALESCE(LOWER(metrics.string.preferences_enhanced_tracking_protection) = "custom", FALSE)
      ) AS INT64
    ) AS etp_custom,
    CAST(
      MAX(
        COALESCE(
          LOWER(metrics.string.preferences_enhanced_tracking_protection) NOT IN (
            'custom',
            'standard',
            'strict'
          ),
          FALSE
        )
      ) AS INT64
    ) AS etp_disabled,
    --Tab count
    MAX(
      COALESCE(metrics.counter.metrics_private_tabs_open_count, 0)
    ) AS metrics_private_tabs_open_count,
    MAX(COALESCE(metrics.counter.metrics_tabs_open_count, 0)) AS metrics_tabs_open_count,
    --Default browser
    CAST(
      MAX(COALESCE(metrics.boolean.metrics_default_browser, FALSE)) AS INT64
    ) AS metrics_default_browser,
    --Awesomebar Location
    CAST(
      MAX(
        COALESCE(metrics.string.preferences_toolbar_position_setting IN ('top', 'fixed_top'), FALSE)
      ) AS INT64
    ) AS awesomebar_top,
    CAST(
      MAX(COALESCE(metrics.string.preferences_toolbar_position_setting = 'bottom', FALSE)) AS INT64
    ) AS awesomebar_bottom,
    CAST(
      MAX(
        COALESCE(
          LOWER(metrics.string.preferences_toolbar_position_setting) NOT IN (
            'top',
            'fixed_top',
            'bottom'
          ),
          FALSE
        )
      ) AS INT64
    ) AS awesomebar_null,
    --Notification
    CAST(
      MAX(COALESCE(metrics.boolean.metrics_notifications_allowed, FALSE)) AS INT64
    ) AS metrics_notifications_allowed,
    CAST(
      MAX(COALESCE(metrics.boolean.events_marketing_notification_allowed, FALSE)) AS INT64
    ) AS events_marketing_notification_allowed,
    --Customize Homepage
    CAST(
      MAX(COALESCE(metrics.boolean.customize_home_contile, FALSE)) AS INT64
    ) AS customize_home_contile,
    CAST(
      MAX(COALESCE(metrics.boolean.customize_home_jump_back_in, FALSE)) AS INT64
    ) AS customize_home_jump_back_in,
    CAST(
      MAX(COALESCE(metrics.boolean.customize_home_most_visited_sites, FALSE)) AS INT64
    ) AS customize_home_most_visited_sites,
    CAST(
      MAX(COALESCE(metrics.boolean.customize_home_pocket, FALSE)) AS INT64
    ) AS customize_home_pocket,
    CAST(
      MAX(COALESCE(metrics.boolean.customize_home_recently_saved, FALSE)) AS INT64
    ) AS customize_home_recently_saved,
    CAST(
      MAX(COALESCE(metrics.boolean.customize_home_recently_visited, FALSE)) AS INT64
    ) AS customize_home_recently_visited
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
