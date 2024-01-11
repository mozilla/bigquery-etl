WITH clients_retention AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    days_seen_bits,
  FROM
    fenix.baseline_clients_last_seen
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
    install_source,
  FROM
    fenix.firefox_android_clients
  WHERE
    -- Two weeks need to elapse before calculating the week 2 retention
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 13 DAY)
    AND channel = "release"
),
retention_calculation AS (
  SELECT
    clients_first_seen.*,
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
      retention.day_13.metric_date <> first_seen_date,
      ERROR("Metric date misaligned with first_seen_date"),
      first_seen_date
    ) AS first_seen_date
  ),
  -- retention UDF works on 0 index basis, that's why for example week_1 is aliased as week 2 to make it a bit more user friendly.
  retention.day_13.active_in_week_1 AS retained_week_2,
FROM
  retention_calculation
