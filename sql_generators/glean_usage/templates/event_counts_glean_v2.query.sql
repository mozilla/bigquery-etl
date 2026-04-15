-- Generated via ./bqetl generate glean_usage
WITH
{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] not in ["telemetry"]
   and dataset['bq_dataset_family'] in event_tables_per_dataset %}
  {% for events_table in event_tables_per_dataset[dataset['bq_dataset_family']] -%}
    {{ dataset['bq_dataset_family'] }}_{{ events_table }} AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "{{ dataset['bq_dataset_family'] }}" AS app_id,
        "{{ dataset['app_name'] }}" AS app_name,
        "{{ dataset['canonical_app_name'] }}" AS normalized_app_name,
        "{{ '_'.join(events_table.split('_')[:-1]) }}" AS ping_type,
        event.category AS event_category,
        event.name AS event_name,
        normalized_channel,
        normalized_country_code,
        client_info.app_display_version AS app_version,
        SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
        COUNT(*) * 10 AS total_events,
      FROM `moz-fx-data-shared-prod.{{ dataset['bq_dataset_family'] }}_stable.{{ events_table }}`
      CROSS JOIN UNNEST(events) AS event
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND sample_id BETWEEN 0 AND 9
      GROUP BY
        submission_date,
        app_id,
        app_name,
        normalized_app_name,
        ping_type,
        event_category,
        event_name,
        normalized_channel,
        normalized_country_code,
        app_version
    )
  {% if not outer_loop.last -%}
    ,
  {% endif -%}
  {% endfor %}
{% endif %}
{% endfor %}
{% endfor %}

{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] not in ["telemetry"]
   and dataset['bq_dataset_family'] in event_tables_per_dataset %}
  {% for events_table in event_tables_per_dataset[dataset['bq_dataset_family']] -%}
    SELECT
      *
    FROM
      {{ dataset['bq_dataset_family'] }}_{{ events_table }}
    {% if not outer_loop.last -%}
      UNION ALL
    {% endif -%}
  {% endfor %}
{% endif %}
{% endfor %}
{% endfor %}
