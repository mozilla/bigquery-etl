-- Generated via ./bqetl generate experiment_monitoring
WITH
{% for app_dataset in applications %}
  {% if app_dataset == "telemetry" %}
    {{ app_dataset }} AS (
      SELECT DISTINCT
        submission_date,
        e.key AS experiment_id,
        e.value AS branch,
        client_id
      FROM
        telemetry.clients_daily
      CROSS JOIN
        UNNEST(experiments) AS e
    )
  {% elif app_dataset == "monitor_cirrus" %}
    {{ app_dataset }} AS (
      SELECT DISTINCT
        DATE(submission_timestamp) AS submission_date,
        e.key AS experiment_id,
        e.value.branch AS branch,
        client_info.client_id
      FROM
        `moz-fx-data-shared-prod.{{ app_dataset }}.enrollment`
      CROSS JOIN
        UNNEST(ping_info.experiments) AS e
    )
  {% else %}
    {{ app_dataset }} AS (
      SELECT DISTINCT
        DATE(submission_timestamp) AS submission_date,
        e.key AS experiment_id,
        e.value.branch AS branch,
        client_info.client_id
      FROM
        `moz-fx-data-shared-prod.{{ app_dataset }}.baseline`
      CROSS JOIN
        UNNEST(ping_info.experiments) AS e
    )
  {% endif %}
  {% if not loop.last %}
    ,
  {% endif %}
{% endfor %}
SELECT
  submission_date,
  experiment_id,
  branch,
  COUNT(*) AS active_clients
FROM
  (
    {% for app_dataset in applications %}
      SELECT
        *
      FROM
        {{ app_dataset }}
      {% if not loop.last %}
        UNION ALL
      {% endif %}
    {% endfor %}
  )
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  experiment_id,
  branch
