-- Generated via ./bqetl generate glean_usage
{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] not in ["telemetry", "accounts_frontend", "accounts_backend"]
   and dataset['bq_dataset_family'] in event_tables_per_dataset %}
  {% if not outer_loop.first -%}
  UNION ALL
  {% endif -%}
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
    -- Aggregates event counts over 60-minute intervals
      INTERVAL(
        DIV(
          EXTRACT(MINUTE FROM submission_timestamp),
          60
        ) * 60
      ) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      INTERVAL(
        (
          DIV(
            EXTRACT(MINUTE FROM submission_timestamp),
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
    channel,
    version,
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
  FROM (
    {% for events_table in event_tables_per_dataset[dataset['bq_dataset_family']] -%}
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      ping_info
    FROM
      `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_stable.{{ events_table }}`
    {{ "UNION ALL" if not loop.last }}
    {% endfor -%}
  )
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
    channel,
    version,
    experiment,
    experiment_branch
{% elif dataset['bq_dataset_family'] in ["accounts_frontend", "accounts_backend"] %}
  {% if not outer_loop.first -%}
  UNION ALL
  {% endif -%}
      -- FxA uses custom pings to send events without a category and extras.
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      -- Aggregates event counts over 60-minute intervals
      INTERVAL(
        DIV(
          EXTRACT(MINUTE FROM submission_timestamp),
          60
        ) * 60
      ) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      INTERVAL(
        (
          DIV(
            EXTRACT(MINUTE FROM submission_timestamp),
            60
          ) + 1
        ) * 60
      ) MINUTE
    ) AS window_end,
    CAST(NULL AS STRING) AS event_category,
    event_name,
    CAST(NULL AS STRING) AS event_extra_key,
    normalized_country_code AS country,
    "{{ dataset['canonical_app_name'] }}" AS normalized_app_name,
    channel,
    version,
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
  FROM (
    {% for events_table in event_tables_per_dataset[dataset['bq_dataset_family']] -%}
    SELECT
      submission_timestamp,
      events,
      metrics.string.event_name,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      ping_info
    FROM
      `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_stable.{{ events_table }}`
    {{ "UNION ALL" if not loop.last }}
    {% endfor -%}
  )
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
    channel,
    version,
    experiment,
    experiment_branch
{% endif %}
{% endfor %}
{% endfor %}
