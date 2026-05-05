-- Generated via ./bqetl generate glean_usage
WITH
{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] not in ["telemetry"]
   and dataset['bq_dataset_family'] in event_tables_per_dataset %}
  {% if dataset['bq_dataset_family'] in manual_refresh_apps -%}
    {{ dataset['bq_dataset_family'] }}_aggregated AS (
      -- Use event_monitoring_live_v1 for apps that use manual refreshes
      SELECT
        DATE(submission_date) AS submission_date,
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
        experiment_branch,
        total_events,
      FROM
        `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_derived.event_monitoring_live_v1`
      WHERE
        DATE(submission_date) = @submission_date
    )
  {% else %}
    {% for events_table in event_tables_per_dataset[dataset['bq_dataset_family']] -%}
      base_{{ dataset['bq_dataset_family'] }}_{{ events_table }} AS (
        SELECT
          DATE(@submission_date) AS submission_date,
          TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
          TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
          event.category AS event_category,
          event.name AS event_name,
          event_extra.key AS event_extra_key,
          normalized_country_code AS country,
          client_info.app_channel AS channel,
          client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
          COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
          COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch, '*') AS experiment_branch,
          COUNT(*) AS total_events,
        FROM
          `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_stable.{{ events_table }}`
        CROSS JOIN
          UNNEST(events) AS event
        CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
          UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
        LEFT JOIN
          -- Add * extra to every event to get total event count
          UNNEST(event.extra || [STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
        WHERE
          DATE(submission_timestamp) = @submission_date
          {% if dataset['app_name'] == "firefox_desktop" and events_table == "events_v1" %}
            -- See https://mozilla-hub.atlassian.net/browse/DENG-9732
            AND (
              event.category = "uptake.remotecontent.result"
              AND event.name IN ("uptake_remotesettings", "uptake_normandy")
              AND mozfun.norm.extract_version(client_info.app_display_version, 'major') >= 143
              AND sample_id != 0
            ) IS NOT TRUE
          {% endif %}
        GROUP BY
          submission_date,
          window_start,
          window_end,
          event_category,
          event_name,
          event_extra_key,
          country,
          channel,
          version,
          experiment,
          experiment_branch
      ),
    {% endfor %}
    {{ dataset['bq_dataset_family'] }}_aggregated AS (
      SELECT
        submission_date,
        window_start,
        window_end,
        event_category,
        event_name,
        event_extra_key,
        country,
        "{{ dataset['canonical_app_name'] }}" AS normalized_app_name,
        channel,
        version,
        experiment,
        experiment_branch,
        SUM(total_events) AS total_events,
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
  {% endif %}
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
