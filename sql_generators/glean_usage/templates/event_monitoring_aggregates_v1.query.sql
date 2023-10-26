-- Generated via ./bqetl generate glean_usage
{% for (dataset, channel) in datasets -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
{% if dataset not in ["telemetry", "accounts_frontend", "accounts_backend"] %}
SELECT
    DATE(submission_timestamp) AS submission_date,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    country,
    normalized_app_name,
    {% if app_name == "fenix" -%}
    mozfun.norm.fenix_app_info("{{ dataset }}", app_build).channel AS normalized_channel,
    {% else %}
    "{{ channel }}" AS normalized_channel,
    {% endif %}
    client_info.app_display_version AS version,
    COUNT(*) AS total_events
  FROM
    `{{ project_id }}.{{ dataset }}_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event,
    UNNEST(event.extra) AS event_extra
{% elif dataset in ["accounts_frontend", "accounts_backend"] %}
-- FxA uses custom pings to send events without a category and extras.
      SELECT
        TIMESTAMP_ADD(
          TIMESTAMP_TRUNC(SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time), HOUR),
    -- Aggregates event counts over 30-minute intervals
          INTERVAL(
            DIV(
              EXTRACT(MINUTE FROM SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time)),
              60
            ) * 60
          ) MINUTE
        ) AS window_start,
        TIMESTAMP_ADD(
          TIMESTAMP_TRUNC(SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time), HOUR),
          INTERVAL(
            (
              DIV(
                EXTRACT(MINUTE FROM SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time)),
                60
              ) + 1
            ) * 60
          ) MINUTE
        ) AS window_end,
        NULL AS event_category,
        metrics.string.event_name,
        NULL AS event_extra_key,
        country,
        normalized_app_name,
        normalized_channel,
        client_info.app_display_version AS VERSION,
        COUNT(*) AS total_events
      FROM
        `{{ project_id }}.{{ dataset }}_stable.accounts_events_v1`
    {% endif %}
    WHERE DATE(submission_timestamp) = @submission_date
  GROUP BY
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    normalized_channel,
    version
{% endfor %}