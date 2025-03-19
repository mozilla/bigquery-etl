-- Generated via ./bqetl generate glean_usage
WITH
{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] not in ["telemetry"]
   and dataset['bq_dataset_family'] in event_tables_per_dataset %}
  {% for events_table in event_tables_per_dataset[dataset['bq_dataset_family']] -%}
    base_{{ dataset['bq_dataset_family'] }}_{{ events_table }} AS (
      SELECT
        submission_timestamp,
        event.category AS event_category,
        event.name AS event_name,
        event_extra.key AS event_extra_key,
        normalized_country_code AS country,
        "{{ dataset['canonical_app_name'] }}" AS normalized_app_name,
        client_info.app_channel AS channel,
        client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
        COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
        COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch, '*') AS experiment_branch,
      FROM
        `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_stable.{{ events_table }}`
      CROSS JOIN
        UNNEST(events) AS event
      CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
        UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
      LEFT JOIN
        UNNEST(event.extra) AS event_extra
    ),
  {% endfor %}
  {{ dataset['bq_dataset_family'] }}_aggregated AS (
    SELECT
      @submission_date AS submission_date,
      TIMESTAMP_ADD(
        TIMESTAMP_TRUNC(submission_timestamp, HOUR),
          -- Aggregates event counts over 60-minute intervals
        INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 60) * 60) MINUTE
      ) AS window_start,
      TIMESTAMP_ADD(
        TIMESTAMP_TRUNC(submission_timestamp, HOUR),
        INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 60) + 1) * 60) MINUTE
      ) AS window_end,
      * EXCEPT (submission_timestamp),
      COUNT(*) AS total_events,
    FROM
      (
        {% for events_table in event_tables_per_dataset[dataset['bq_dataset_family']] -%}
          SELECT
            *
          FROM
            base_{{ dataset['bq_dataset_family'] }}_{{ events_table }}
          {{ "UNION ALL" if not loop.last }}
        {% endfor -%}
      )
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
  )
  {% if not outer_loop.last -%}
  ,
  {% endif -%}
{% endif %}
{% endfor %}
{% endfor %}

{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] not in ["telemetry"]
   and dataset['bq_dataset_family'] in event_tables_per_dataset %}
  SELECT
    *
  FROM
    {{ dataset['bq_dataset_family'] }}_aggregated
  {% if not outer_loop.last -%}
  UNION ALL
  {% endif -%}
{% endif %}
{% endfor %}
{% endfor %}
