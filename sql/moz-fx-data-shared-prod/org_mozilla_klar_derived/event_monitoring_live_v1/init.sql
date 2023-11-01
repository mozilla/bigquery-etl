CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.org_mozilla_klar_derived.event_monitoring_live_v1`
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 60)
  AS
  SELECT
    DATE(submission_timestamp) AS submission_date,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(
        TIMESTAMP_ADD(
          SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time),
          INTERVAL event.timestamp MILLISECOND
        ),
        HOUR
      ),
        -- Aggregates event counts over 30-minute intervals
      INTERVAL(
        DIV(
          EXTRACT(
            MINUTE
            FROM
              TIMESTAMP_ADD(
                SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time),
                INTERVAL event.timestamp MILLISECOND
              )
          ),
          60
        ) * 60
      ) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(
        TIMESTAMP_ADD(
          SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time),
          INTERVAL event.timestamp MILLISECOND
        ),
        HOUR
      ),
      INTERVAL(
        (
          DIV(
            EXTRACT(
              MINUTE
              FROM
                TIMESTAMP_ADD(
                  SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time),
                  INTERVAL event.timestamp MILLISECOND
                )
            ),
            60
          ) + 1
        ) * 60
      ) MINUTE
    ) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox Klar for Android' AS normalized_app_name,
    normalized_channel,
    client_info.app_display_version AS version,
    COUNT(*) AS total_events
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_live.events_v1`
  CROSS JOIN
    UNNEST(events) AS event,
    UNNEST(event.extra) AS event_extra
  WHERE
    DATE(submission_timestamp) >= "2023-11-01"
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    normalized_channel,
    version
