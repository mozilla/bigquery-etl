-- Generated via ./bqetl generate glean_usage
{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] not in ["telemetry", "accounts_frontend", "accounts_backend"] %}
  {% if not outer_loop.first -%}
  UNION ALL
  {% endif -%}
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time), HOUR),
    -- Aggregates event counts over 60-minute intervals
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
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "{{ dataset['canonical_app_name'] }}" AS normalized_app_name,
    {% if app_name == "fenix" -%}
    mozfun.norm.fenix_app_info("{{ dataset['bq_dataset_family'] }}", app_build).channel AS normalized_channel,
    {% else %}
    "{{ dataset.get('app_channel', 'release') }}" AS normalized_channel,
    {% endif %}
    client_info.app_display_version AS version,
    -- Access experiment information.
    -- Additional iteration is necessary to aggregate total event count across experiments
    -- which is denoted with "*".
    -- Some clients are enrolled in multiple experiments, so simply summing up the totals 
    -- across all the experiments would double count events.
    CASE 
      experiment_index 
    WHEN 
      ARRAY_LENGTH(ping_info.experiments) 
    THEN 
      "*" 
    ELSE 
      ping_info.experiments[SAFE_OFFSET(experiment_index)].key 
    END AS experiment,
    CASE 
      experiment_index 
    WHEN 
      ARRAY_LENGTH(ping_info.experiments) 
    THEN 
      "*" 
    ELSE 
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch 
    END AS experiment_branch,
    COUNT(*) AS total_events
  FROM
    `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
  WHERE 
    DATE(submission_timestamp) = @submission_date
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
    version,
    experiment,
    experiment_branch
{% elif dataset in ["accounts_frontend", "accounts_backend"] %}
  {% if not outer_loop.first -%}
  UNION ALL
  {% endif -%}
      -- FxA uses custom pings to send events without a category and extras.
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time), HOUR),
      -- Aggregates event counts over 60-minute intervals
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
    normalized_country_code AS country,
    "{{ dataset['canonical_app_name'] }}" AS normalized_app_name,
    normalized_channel,
    client_info.app_display_version AS version,
    -- Access experiment information.
    -- Additional iteration is necessary to aggregate total event count across experiments
    -- which is denoted with "*".
    -- Some clients are enrolled in multiple experiments, so simply summing up the totals 
    -- across all the experiments would double count events.
    CASE 
      experiment_index 
    WHEN 
      ARRAY_LENGTH(ping_info.experiments) 
    THEN 
      "*" 
    ELSE 
      ping_info.experiments[SAFE_OFFSET(experiment_index)].key 
    END AS experiment,
    CASE 
      experiment_index 
    WHEN 
      ARRAY_LENGTH(ping_info.experiments) 
    THEN 
      "*" 
    ELSE 
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch 
    END AS experiment_branch,
    COUNT(*) AS total_events
  FROM
    `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_stable.accounts_events_v1`
  CROSS JOIN
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  WHERE 
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    normalized_channel,
    version,
    experiment,
    experiment_branch
{% endif %}
{% endfor %}
{% endfor %}