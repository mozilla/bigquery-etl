CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ target_view }}` AS
  {% for app in apps %}
    {% set outer_loop = loop -%}
    {% for dataset in app -%}
      {% if dataset['bq_dataset_family'] in prod_datasets
        and dataset['bq_dataset_family'] in event_tables_per_dataset %}
        SELECT
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
          total_events
        FROM
          `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_derived.event_monitoring_live_v1`
        WHERE
          -- workaround for event_monitoring_aggregates outage https://bugzilla.mozilla.org/show_bug.cgi?id=1989142
          -- DATE(submission_date) > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
          DATE(submission_date) > '2025-09-15'
        UNION ALL
      {% endif %}
    {% endfor %}
  {% endfor %}
SELECT
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
  total_events
FROM
  `{{ project_id }}.{{ target_table }}`
WHERE
  -- workaround for event_monitoring_aggregates outage https://bugzilla.mozilla.org/show_bug.cgi?id=1989142
  -- DATE(submission_date) <= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  DATE(submission_date) <= '2025-09-15'
