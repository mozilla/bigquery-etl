WITH baseline AS (
  SELECT
    *
  FROM
    `{{ project_id }}.{{ app_name }}.baseline_clients_last_seen`
  WHERE
    submission_date = @submission_date
),
metrics AS (
  SELECT
    *
  FROM
    `{{ project_id }}.{{ app_name }}.metrics_clients_last_seen`
  WHERE
    -- The join between baseline and metrics pings is based on submission_date with a 1 day delay,
    -- since metrics pings usually arrive within 1 day after their logical activity period.
    submission_date = DATE_ADD(@submission_date, INTERVAL 1 DAY)
)
SELECT
  baseline.client_id,
  baseline.sample_id,
  baseline.submission_date,
  baseline.normalized_channel,
  * EXCEPT(submission_date, normalized_channel, client_id, sample_id)
FROM
  baseline
LEFT JOIN metrics
ON baseline.client_id = metrics.client_id AND
  baseline.sample_id = metrics.sample_id AND
  (
    baseline.normalized_channel = metrics.normalized_channel OR
    (baseline.normalized_channel IS NULL AND metrics.normalized_channel IS NULL)
  )
-- In some rare cases we end up with the same client_id in multiple channels
-- In those cases, this union can result in client_id duplicates.
-- This logic ensures that the resulting table only includes the client from the channel
-- we've seen first.
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      client_id
    ORDER BY
      first_seen_date ASC
) = 1
