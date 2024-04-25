CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_last_seen_v2`
AS
WITH days_since AS (
  SELECT
    DATE_DIFF(submission_date, first_seen_date, DAY) AS days_since_first_seen,
    DATE_DIFF(submission_date, second_seen_date, DAY) AS days_since_second_seen,
    mozfun.bits28.days_since_seen(days_seen_bits) AS days_since_seen,
    mozfun.bits28.days_since_seen(days_visited_1_uri_bits) AS days_since_visited_1_uri,
    mozfun.bits28.days_since_seen(days_visited_5_uri_bits) AS days_since_visited_5_uri,
    mozfun.bits28.days_since_seen(days_visited_10_uri_bits) AS days_since_visited_10_uri,
    mozfun.bits28.days_since_seen(days_had_8_active_ticks_bits) AS days_since_had_8_active_ticks,
    mozfun.bits28.days_since_seen(days_opened_dev_tools_bits) AS days_since_opened_dev_tools,
    mozfun.bits28.days_since_seen(days_created_profile_bits) AS days_since_created_profile,
    mozfun.bits28.days_since_seen(days_interacted_bits) AS days_since_interacted,
    mozfun.bits28.days_since_seen(
      days_visited_1_uri_bits & days_interacted_bits
    ) AS days_since_qualified_use_v1,
    mozfun.bits28.days_since_seen(
      days_visited_1_uri_normal_mode_bits
    ) AS days_since_visited_1_uri_normal_mode,
    mozfun.bits28.days_since_seen(
      days_visited_1_uri_private_mode_bits
    ) AS days_since_visited_1_uri_private_mode,
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_v2`
)
SELECT
  cls.* EXCEPT (app_name),
  CASE
    WHEN cls.isp_name = 'BrowserStack'
      THEN CONCAT(cls.app_name, ' ', cls.isp_name)
    WHEN distribution_id = 'MozillaOnline'
      THEN CONCAT(cls.app_name, ' ', cls.distribution_id)
    ELSE cls.app_name
  END AS app_name,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_active_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_seen_bits) < 28, FALSE) AS is_monthly_user,
  IF(
    LOWER(IFNULL(cls.isp_name, '')) <> "browserstack"
    AND LOWER(IFNULL(cls.distribution_id, '')) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_desktop
FROM
  days_since cls
