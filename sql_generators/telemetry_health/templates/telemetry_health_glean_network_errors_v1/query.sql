-- Query for telemetry health glean network errors across all applications
{% for app_id in applications %}
(
  WITH sample AS (
    SELECT
      "{{app_id}}" as app_id,
      "{{ application_names[app_id] }}" AS application,
      client_info.client_id,
      {% if application_names[app_id] == "Firefox for Android" %}
      mozfun.norm.fenix_app_info("{{ app_id }}", client_info.app_build).channel AS normalized_channel,
      {% else %}
      COALESCE(normalized_channel, 'Other') AS normalized_channel,
      {% endif %}
      DATE(submission_timestamp) AS submission_date,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
    FROM
      `{{ project_id }}.{{ app_id }}_stable.metrics_v1`
    WHERE
      DATE(submission_timestamp) = @submission_date
  ),
  network_errors AS (
    SELECT
      submission_date,
      app_id,
      application,
      normalized_channel,
      label.key AS error_type,
      SUM(label.value) AS counts,
      COUNT(DISTINCT client_id) AS affected_clients
    FROM
      sample
    CROSS JOIN
      UNNEST(glean_upload_ping_upload_failure) AS label
    GROUP BY ALL
  ),
  all_clients AS (
    SELECT
      submission_date,
      app_id,
      application,
      normalized_channel,
      COUNT(DISTINCT client_id) AS total_clients
    FROM
      sample
    GROUP BY ALL
  )

  SELECT
    d.submission_date,
    d.app_id,
    d.application,
    d.normalized_channel,
    d.error_type,
    d.counts,
    d.affected_clients,
    d.affected_clients / c.total_clients AS fraction_affected_clients,
    c.total_clients,
  FROM network_errors d
  INNER JOIN all_clients c
    USING (submission_date, app_id, application, normalized_channel)
)
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
