/*

This query carries information about each client over a 28-day window,
encoding various forms of usage over those 28 days as bit patterns.

For Glean, the concept of a user being active or not is determined only
from baseline pings; we also record some dimensions here based on metrics
pings, but we are careful to keep the baseline info separate from the
metrics info so that metrics pings only have the effect of providing
updated dimension values, but never have the effect of marking a user as
active for a given day.

*/
WITH cls_yesterday AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    clients_last_seen_v1 AS cls
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
),
--
cls_today AS (
  SELECT
    client_id,
    sample_id,
    baseline_today.normalized_channel,
    baseline_today.client_id IS NOT NULL AS baseline_received_today,
    metrics_today.client_id IS NOT NULL AS metrics_received_today,
    (
      SELECT AS STRUCT
        -- In this raw table, we capture the history of activity over the past
        -- 28 days for each usage criterion as a single 64-bit integer. The
        -- rightmost bit represents whether the user was active in the current day.
        CAST(baseline_today.client_id IS NOT NULL AS INT64) AS days_seen_bits,
        baseline_today.* EXCEPT (submission_date, client_id, sample_id, normalized_channel)
    ) AS baseline,
    (SELECT AS STRUCT metrics_today.* EXCEPT (submission_date, client_id, sample_id)) AS metrics,
  FROM
    baseline_daily_v1 AS baseline_today
  FULL JOIN
    metrics_daily_v1 AS metrics_today
  USING
    (submission_date, client_id, sample_id)
  WHERE
    submission_date = @submission_date
),
  --
adjacent_days_combined AS (
  SELECT
    @submission_date AS submission_date,
    client_id,
    sample_id,
    COALESCE(cls_today.normalized_channel, cls_yesterday.normalized_channel) AS normalized_channel,
    (
      SELECT AS STRUCT
        IF(baseline_received_today, cls_today.baseline, cls_yesterday.baseline).* REPLACE (
          udf.combine_adjacent_days_28_bits(
            cls_yesterday.baseline.days_seen_bits,
            cls_today.baseline.days_seen_bits
          ) AS days_seen_bits,
          udf.combine_adjacent_days_28_bits(
            cls_yesterday.baseline.days_seen_session_start_bits,
            cls_today.baseline.days_seen_session_start_bits
          ) AS days_seen_session_start_bits,
          udf.combine_adjacent_days_28_bits(
            cls_yesterday.baseline.days_seen_session_end_bits,
            cls_today.baseline.days_seen_session_end_bits
          ) AS days_seen_session_end_bits
        )
    ) AS baseline,
    IF(metrics_received_today, cls_today.metrics, cls_yesterday.metrics) AS metrics,
  FROM
    cls_today
  FULL JOIN
    cls_yesterday
  USING
    (client_id, sample_id)
)
SELECT
  *
FROM
  adjacent_days_combined
WHERE
  baseline.days_seen_bits > 0
