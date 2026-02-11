-- Get clients 9 days after they were seen for the first time.
WITH fenix_new_profiles AS (
  SELECT
    client_id,
    first_seen_date,
    country,
    REGEXP_EXTRACT(play_store_attribution_install_referrer_response, r'gclid=([^&]+)') AS gclid,
  FROM
    `moz-fx-data-shared-prod.fenix.new_profile_clients`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 8 DAY)
    AND normalized_channel = 'release'
    AND install_source = 'com.android.vending'
),
-- marketing_card_optin_first_week indicates who opt-in to the marketing card in the first week
marketing_card_optin_clients AS (
  SELECT DISTINCT
    client_id,
  FROM
    `moz-fx-data-shared-prod.fenix.events_stream`
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 8 DAY)
    AND DATE_SUB(@submission_date, INTERVAL 2 DAY)
    AND normalized_channel = 'release'
    AND mozfun.norm.browser_version_info(client_info.app_display_version).major_version >= 136
    AND (
      event_category = 'onboarding'
      OR event_name IN (
        'set_as_default_browser_native_prompt_shown',
        'default_browser_changed_via_native_system_prompt'
      )
    )
    AND (
      event_name = 'marketing_data_continue_clicked'
      OR JSON_VALUE(event_extra, '$.opt_in') = 'true'
    )
),
-- Get clients activity 9 days after they were seen for the first time.
first_week_active_days AS (
  SELECT
    client_id,
    CAST(mozfun.bits28.active_in_range(days_active_bits, -8, 1) AS INTEGER) AS active_d0,
    CAST(mozfun.bits28.active_in_range(days_active_bits, -7, 1) AS INTEGER) AS active_d1,
    CAST(mozfun.bits28.active_in_range(days_active_bits, -6, 1) AS INTEGER) AS active_d2,
    CAST(mozfun.bits28.active_in_range(days_active_bits, -5, 1) AS INTEGER) AS active_d3,
    CAST(mozfun.bits28.active_in_range(days_active_bits, -4, 1) AS INTEGER) AS active_d4,
    CAST(mozfun.bits28.active_in_range(days_active_bits, -3, 1) AS INTEGER) AS active_d5,
    CAST(mozfun.bits28.active_in_range(days_active_bits, -2, 1) AS INTEGER) AS active_d6,
  FROM
    `moz-fx-data-shared-prod.fenix.active_users`
  WHERE
    submission_date = @submission_date
    AND first_seen_date = DATE_SUB(@submission_date, INTERVAL 8 DAY)
    AND normalized_channel = 'release'
),
final AS (
  SELECT
    @submission_date AS report_date,
    first_seen_date,
    DATE_ADD(first_seen_date, INTERVAL 6 DAY) AS first_week_end_date,
    client_id,
    country,
    gclid,
    COALESCE(
      CAST((marketing_card_optin_clients.client_id IS NOT NULL) AS INTEGER),
      0
    ) AS marketing_card_optin_first_week,
    COALESCE(active_d0, 0) AS active_d0,
    COALESCE(active_d1, 0) AS active_d1,
    COALESCE(active_d2, 0) AS active_d2,
    COALESCE(active_d3, 0) AS active_d3,
    COALESCE(active_d4, 0) AS active_d4,
    COALESCE(active_d5, 0) AS active_d5,
    COALESCE(active_d6, 0) AS active_d6,
  FROM
    fenix_new_profiles
  LEFT JOIN
    first_week_active_days
    USING (client_id)
  LEFT JOIN
    marketing_card_optin_clients
    USING (client_id)
)
SELECT
  *,
  CAST(((active_d4 + active_d5 + active_d6) > 0) AS INTEGER) AS activation_1,
  CAST(
    ((active_d1 + active_d2 + active_d3) > 1 AND (active_d4 + active_d5 + active_d6) > 1) AS INTEGER
  ) AS activation_2,
FROM
  final
