WITH clients_retention AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    days_seen_bits,
  FROM
    firefox_ios.baseline_clients_last_seen
  WHERE
    submission_date = @submission_date
    AND normalized_channel = "release"
),
clients_first_seen AS (
  SELECT
    first_seen_date,
    client_id,
    sample_id,
    first_reported_country,
    first_reported_isp,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
  FROM
    firefox_ios.firefox_ios_clients
  WHERE
    -- 28 days need to elapse before calculating the week 4 and day 28 retention metrics
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND channel = "release"
    -- filtering out suspicious devices on iOS, for more info see: bug-1846554
    AND NOT is_suspicious_device_client
),
retention_calculation AS (
  SELECT
    clients_first_seen.*,
    BIT_COUNT(clients_retention.days_seen_bits) AS days_seen_in_first_28_days,
    mozfun.bits28.retention(clients_retention.days_seen_bits, @submission_date) AS retention,
  FROM
    clients_retention
  INNER JOIN
    clients_first_seen
    USING (client_id, sample_id)
)
SELECT
  @submission_date AS submission_date,
  * EXCEPT (retention) REPLACE(
    -- metric date should align with first_seen_date, if that is not the case then the query will fail.
    IF(
      retention.day_27.metric_date <> first_seen_date,
      ERROR("Metric date misaligned with first_seen_date"),
      first_seen_date
    ) AS first_seen_date
  ),
  days_seen_in_first_28_days > 1 AS repeat_first_month_user,
  -- retention UDF works on 0 index basis, that's why week_3 is aliased as week 4 to make it a bit more user friendly.
  retention.day_27.active_in_week_3 AS retained_week_4,
FROM
  retention_calculation
