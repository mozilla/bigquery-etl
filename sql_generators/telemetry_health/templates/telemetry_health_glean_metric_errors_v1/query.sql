-- Query for telemetry health glean metric errors across all applications
{% for app_id in applications %}
(
  WITH sample AS (
    SELECT
      "{{ application_names[app_id] }}" AS application,
      client_info.client_id,
      {% if application_names[app_id] == "Firefox for Android" %}
      mozfun.norm.fenix_app_info("{{ app_id }}", client_info.app_build).channel AS normalized_channel,
      {% else %}
      COALESCE(normalized_channel, 'Other') AS normalized_channel,
      {% endif %}
      DATE(submission_timestamp) AS submission_date,
      metrics,
    FROM
      `{{ project_id }}.{{ app_id }}_stable.metrics_v1`
    WHERE
      DATE(submission_timestamp) = @submission_date
  ),
  errors AS (
    SELECT
      submission_date,
      application,
      normalized_channel,
      e.error_type,
      label.key AS metric,
      SUM(label.value) AS counts,
      COUNT(DISTINCT client_id) AS affected_clients
    FROM
      sample
    CROSS JOIN
      UNNEST([
        STRUCT('Invalid Label' AS error_type, metrics.labeled_counter.glean_error_invalid_label AS errors),
        ('Invalid Value', metrics.labeled_counter.glean_error_invalid_value),
        ('Invalid State', metrics.labeled_counter.glean_error_invalid_state),
        ('Invalid Overflow', metrics.labeled_counter.glean_error_invalid_overflow)
      ]) AS e
    CROSS JOIN UNNEST(e.errors) AS label
    GROUP BY ALL
  ),
  all_clients AS (
    SELECT
      submission_date,
      application,
      normalized_channel,
      COUNT(DISTINCT client_id) AS total_clients
    FROM
      sample
    GROUP BY ALL
  )

  SELECT
    d.submission_date,
    d.application,
    d.normalized_channel,
    d.error_type,
    d.metric,
    d.counts,
    d.affected_clients,
    d.affected_clients / c.total_clients AS fraction_clients_affected,
    c.total_clients,
  FROM errors d
  INNER JOIN all_clients c
    USING (submission_date, application, normalized_channel)
)
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
