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
-- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND udf.shift_28_bits_one_day(days_seen_bits) > 0
),
--
cls_today AS (
  SELECT
    client_id,
    sample_id,
    baseline_today.normalized_channel,
-- In this raw table, we capture the history of activity over the past
-- 28 days for each usage criterion as a single 64-bit integer. The
-- rightmost bit represents whether the user was active in the current day.
    CAST(baseline_today.client_id IS NOT NULL AS INT64) AS days_seen_bits,
    (SELECT AS STRUCT baseline_today.* EXCEPT (submission_date, client_id, sample_id)) AS baseline,
    (SELECT AS STRUCT metrics_today.* EXCEPT (submission_date, client_id, sample_id)) AS metrics,
  FROM
    baseline_daily_v1 AS baseline_today
  FULL JOIN
    metrics_daily_v1 AS metrics_today
  USING
    (submission_date, client_id, sample_id)
  WHERE
    submission_date = @submission_date
)
  --
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  COALESCE(cls_today.normalized_channel, cls_yesterday.normalized_channel) AS normalized_channel,
  udf.combine_adjacent_days_28_bits(
    cls_yesterday.days_seen_bits,
    cls_today.days_seen_bits
  ) AS days_seen_bits,
  IF(cls_today.baseline IS NOT NULL, cls_today.baseline, cls_yesterday.baseline) AS baseline,
  IF(cls_today.metrics IS NOT NULL, cls_today.metrics, cls_yesterday.metrics) AS metrics,
FROM
  cls_today
FULL JOIN
  cls_yesterday
USING
  (client_id, sample_id)
