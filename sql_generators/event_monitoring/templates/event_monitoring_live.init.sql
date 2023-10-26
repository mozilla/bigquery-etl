  CREATE MATERIALIZED VIEW
  IF
    NOT EXISTS {{ project_id }}.{{ dataset_id }}_derived.event_monitoring_live
    OPTIONS
      (enable_refresh = TRUE, refresh_interval_minutes = 60)
    AS
{% if dataset_id not in ["telemetry", "accounts_frontend_live", "accounts_backend_live"] %}
SELECT
  TIMESTAMP_ADD( TIMESTAMP_TRUNC(TIMESTAMP_ADD(SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time), INTERVAL event.timestamp MILLISECOND), HOUR),
    -- Aggregates event counts over 30-minute intervals
    INTERVAL(DIV(EXTRACT(MINUTE
        FROM
          TIMESTAMP_ADD(SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time), INTERVAL event.timestamp MILLISECOND)), 60) * 60) MINUTE ) AS window_start,
  TIMESTAMP_ADD( TIMESTAMP_TRUNC(TIMESTAMP_ADD(SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time), INTERVAL event.timestamp MILLISECOND), HOUR), INTERVAL((DIV(EXTRACT(MINUTE
          FROM
            TIMESTAMP_ADD(SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time), INTERVAL event.timestamp MILLISECOND)), 60) + 1) * 60) MINUTE ) AS window_end,
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_channel,
  client_info.app_display_version AS version,
  COUNT(*) AS total_events
FROM
  `{{ project_id }}.{{ dataset_id }}_live.events_v1`
CROSS JOIN
  UNNEST(events) AS event,
  UNNEST(event.extra) AS event_extra
{% elif dataset_id in ["accounts_frontend", "accounts_backend"] %}
-- FxA uses custom pings to send events without a category and extras.
SELECT
  TIMESTAMP_ADD(TIMESTAMP_TRUNC(SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time), HOUR),
    -- Aggregates event counts over 30-minute intervals
    INTERVAL(DIV(EXTRACT(MINUTE
        FROM
          SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time)), 60) * 60) MINUTE) AS window_start,
  TIMESTAMP_ADD(TIMESTAMP_TRUNC(SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time), HOUR), INTERVAL((DIV(EXTRACT(MINUTE
          FROM SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time)), 60) + 1) * 60) MINUTE) AS window_end,
  NULL AS event_category,
  metrics.string.event_name,
  NULL AS event_extra_key,
  normalized_channel,
  client_info.app_display_version AS VERSION,
  COUNT(*) AS total_events
FROM
  `{{ project_id }}.{{ dataset_id }}_live.accounts_events_v1`
{% else %}
  TIMESTAMP_ADD( TIMESTAMP_TRUNC(TIMESTAMP_ADD(submission_timestamp, INTERVAL event.f0_ MILLISECOND), HOUR),
    -- Aggregates event counts over 30-minute intervals
    INTERVAL(DIV(EXTRACT(MINUTE
        FROM
          TIMESTAMP_ADD(submission_timestamp, INTERVAL event.f0_ MILLISECOND)), 30) * 30) MINUTE ) AS window_start,
  TIMESTAMP_ADD( TIMESTAMP_TRUNC(TIMESTAMP_ADD(submission_timestamp, INTERVAL event.f0_ MILLISECOND), HOUR), INTERVAL((DIV(EXTRACT(MINUTE
          FROM
            TIMESTAMP_ADD(submission_timestamp, INTERVAL event.f0_ MILLISECOND)), 30) + 1) * 30) MINUTE ) AS window_end,

      event.f2_ AS event_name,
      event.f1_ AS event_category,
      event_map_value.key = 'branch' AS event_extra_key,
      normalized_channel,
      application.version AS version,
      COUNT(*) AS total_events
    FROM
      `moz-fx-data-shared-prod.telemetry_live.event_v4`
    CROSS JOIN
      UNNEST(
        ARRAY_CONCAT(
          payload.events.parent,
          payload.events.content,
          payload.events.dynamic,
          payload.events.extension,
          payload.events.gpu
        )
      ) AS event
    CROSS JOIN
      UNNEST(event.f5_) AS event_map_value
{% endif %}
WHERE
  DATE(submission_timestamp) > "2023-10-23"
GROUP BY
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  normalized_channel,
  version