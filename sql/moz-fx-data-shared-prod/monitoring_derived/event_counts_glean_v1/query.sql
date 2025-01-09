WITH event_counts_per_app_channel AS (
  SELECT
    submission_date,
    event_category,
    event_name,
    event_extra_key,
    normalized_app_name,
    mozfun.norm.app_channel(channel) AS channel,
    country,
    version,
    SUM(total_events) AS total_events,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.event_monitoring_aggregates_v1`
  WHERE
    submission_date = @submission_date
    AND experiment = "*"
    AND experiment_branch = "*"
  GROUP BY
    submission_date,
    event_category,
    event_name,
    event_extra_key,
    normalized_app_name,
    channel,
    country,
    version
  QUALIFY
    -- Get total events by summing events with extras and events with no extras
    FIRST_VALUE(event_extra_key IGNORE NULLS) OVER (
      PARTITION BY
        submission_date,
        event_category,
        event_name,
        normalized_app_name,
        channel,
        country,
        version
      ORDER BY
        total_events DESC
    ) = event_extra_key
    OR event_extra_key IS NULL
)
SELECT
  submission_date,
  event_category,
  event_name,
  normalized_app_name,
  channel,
  country,
  version,
  SUM(total_events) AS total_events,
FROM
  event_counts_per_app_channel
GROUP BY
  submission_date,
  event_category,
  event_name,
  normalized_app_name,
  channel,
  country,
  version
