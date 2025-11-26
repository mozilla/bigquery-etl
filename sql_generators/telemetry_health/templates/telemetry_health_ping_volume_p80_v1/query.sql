-- Query for telemetry health ping volume p80 across all applications
{% for app in applications %}
(
  WITH sample AS (
    SELECT
      "{{ application_names[app] }}" AS application,
      normalized_channel AS channel,
      DATE(submission_timestamp) AS submission_date,
      client_info.client_id,
      COUNT(1) AS ping_count
    FROM
      `{{ project_id }}.{{ app }}.baseline`
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
