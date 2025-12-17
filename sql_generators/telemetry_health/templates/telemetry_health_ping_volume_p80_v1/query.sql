-- Query for telemetry health ping volume p80 across all applications
{% for app_id in applications %}
(
  WITH sample AS (
    SELECT
      "{{ application_names[app_id] }}" AS application,
      {% if application_names[app_id] == "Firefox for Android" %}
      mozfun.norm.fenix_app_info("{{ app_id }}", client_info.app_build).channel AS channel,
      {% else %}
      normalized_channel AS channel,
      {% endif %}
      DATE(submission_timestamp) AS submission_date,
      client_info.client_id,
      COUNT(1) AS ping_count
    FROM
      `{{ project_id }}.{{ app_id }}_stable.baseline_v1`
    WHERE
      sample_id = 0
      AND DATE(submission_timestamp) = @submission_date
    GROUP BY
      application,
      channel,
      submission_date,
      client_info.client_id
  ),
  ping_count_quantiles AS (
    SELECT
      application,
      channel,
      submission_date,
      APPROX_QUANTILES(ping_count, 100) AS quantiles,
    FROM
      sample
    GROUP BY
      application,
      channel,
      submission_date
  )
  SELECT
    application,
    channel,
    submission_date,
    quantiles[OFFSET(80)] AS p80
  FROM
    ping_count_quantiles
)
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
