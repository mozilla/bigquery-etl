WITH new_profiles AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.glean_telemetry.new_profile_churn_clients`
  WHERE
    cohort_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 775 DAY)
    AND @submission_date
    -- DENG-11590 bot profiles stay in the client tables for ad hoc analysis
    AND NOT is_desktop_argument_profile_automation
),
cohort_counts AS (
  SELECT
    cohort_date,
    country,
    locale,
    distribution_id,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    os_version_major,
    os_version_minor,
    app_display_version,
    app_name,
    first_seen_attribution_campaign,
    first_seen_attribution_content,
    first_seen_attribution_medium,
    first_seen_attribution_source,
    is_default_browser,
    -- Every client in a cohort has the same days_data_available; complete cohorts read 31
    MIN(days_data_available) AS days_data_available,
    -- One row per client per cohort, so this is the number of clients
    COUNT(*) AS cohort_clients_size,
    -- Each count is NULL until the cohort has the days of data its flag needs
    IF(
      COUNT(immediately_churned) = 0,
      NULL,
      COUNTIF(immediately_churned)
    ) AS clients_churned_immediately,
    IF(
      COUNT(churned_after_1_day) = 0,
      NULL,
      COUNTIF(churned_after_1_day)
    ) AS clients_churned_after_1_day,
    IF(
      COUNT(churned_after_2_days) = 0,
      NULL,
      COUNTIF(churned_after_2_days)
    ) AS clients_churned_after_2_days,
    IF(
      COUNT(churn_risk_after_5_days) = 0,
      NULL,
      COUNTIF(churn_risk_after_5_days)
    ) AS clients_churn_risk_after_5_days,
    IF(
      COUNT(churn_risk_after_7_days) = 0,
      NULL,
      COUNTIF(churn_risk_after_7_days)
    ) AS clients_churn_risk_after_7_days,
    IF(COUNT(day_0) = 0, NULL, COUNTIF(NOT day_0)) AS clients_inactive_first_day
  FROM
    new_profiles
  GROUP BY
    cohort_date,
    country,
    locale,
    distribution_id,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    os_version_major,
    os_version_minor,
    app_display_version,
    app_name,
    first_seen_attribution_campaign,
    first_seen_attribution_content,
    first_seen_attribution_medium,
    first_seen_attribution_source,
    is_default_browser
)
SELECT
  cohort_date,
  country,
  locale,
  distribution_id,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  os_version_major,
  os_version_minor,
  app_display_version,
  app_name,
  first_seen_attribution_campaign,
  first_seen_attribution_content,
  first_seen_attribution_medium,
  first_seen_attribution_source,
  is_default_browser,
  days_data_available,
  cohort_clients_size,
  clients_churned_immediately,
  clients_churned_after_1_day,
  clients_churned_after_2_days,
  clients_churn_risk_after_5_days,
  clients_churn_risk_after_7_days,
  clients_inactive_first_day,
  -- The churn groups are disjoint, and a NULL part keeps the sum NULL
  clients_churned_immediately + clients_churned_after_1_day AS cumulative_clients_churned_after_1_day,
  clients_churned_immediately + clients_churned_after_1_day + clients_churned_after_2_days AS cumulative_clients_churned_after_2_days
FROM
  cohort_counts
