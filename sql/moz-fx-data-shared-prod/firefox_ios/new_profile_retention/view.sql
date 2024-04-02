CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.new_profile_retention`
AS
WITH new_profiles AS (
  SELECT
    first_seen_date,
    client_id,
    sample_id,
    channel,
    country,
    isp,
    -- TODO: attribution will need to come from an attribution table
    -- adjust_ad_group,
    -- adjust_campaign,
    -- adjust_creative,
    -- adjust_network,
  FROM
    firefox_ios.new_profiles
  WHERE
    -- 28 days need to elapse before calculating the week 4 and day 28 retention metrics
    -- first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
    -- AND channel = "release"
    -- filtering out suspicious devices on iOS, for more info see: bug-1846554
    NOT is_suspicious_device_client
),
profile_activity AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    "release" AS channel, -- TODO: remove the value once added to the view.
    days_seen_bits,
  FROM
    backfills_staging_derived.active_users -- TODO: update once the view has been finalized and deployed to production
  WHERE
    -- submission_date = @submission_date
    app_name = "Firefox iOS"
),
retention_calculation AS (
  SELECT
    new_profiles.*,
    BIT_COUNT(profile_activity.days_seen_bits) AS days_seen_in_first_28_days,
    -- mozfun.bits28.retention(profile_activity.days_seen_bits, @submission_date) AS retention,
        mozfun.bits28.retention(profile_activity.days_seen_bits, profile_activity.submission_date) AS retention,
  FROM
    new_profiles
  LEFT JOIN
    profile_activity
      ON new_profiles.client_id = profile_activity.client_id
        AND new_profiles.sample_id = profile_activity.sample_id
        AND new_profiles.channel = profile_activity.channel
        AND new_profiles.first_seen_date = DATE_SUB(profile_activity.submission_date, INTERVAL 27 DAY)
)
SELECT
  * EXCEPT (retention) REPLACE(
    -- metric date should align with first_seen_date, if that is not the case then the query will fail.
    IF(
      retention.day_27.metric_date <> first_seen_date,
      ERROR("Metric date misaligned with first_seen_date"),
      first_seen_date
    ) AS first_seen_date
  ),
  COALESCE(days_seen_in_first_28_days > 1, FALSE) AS repeat_first_month_user,
  -- retention UDF works on 0 index basis, that's why week_3 is aliased as week 4 to make it a bit more user friendly.
  COALESCE(retention.day_27.active_in_week_3, FALSE) AS retained_week_4,
FROM
  retention_calculation
