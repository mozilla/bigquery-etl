{% for (dataset, channel) in datasets -%}
  {% if not loop.first -%}
  UNION ALL
  {% endif -%}
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    sample_id,
    {% if app_name == "fenix" -%}
    mozfun.norm.fenix_app_info("{{ dataset }}", client_info.app_build).channel AS normalized_channel,
    {% else -%}
    "{{ channel }}" AS normalized_channel,
    {% endif -%}
    COUNT(*) AS n_metrics_ping,
    1 AS days_sent_metrics_ping_bits,
    {% if app_name in metrics -%}
    {% for metric in metrics[app_name] -%}
    {{ metrics[app_name][metric].sql }} AS {{ metric }},
    {% endfor -%}
    {% endif -%}
  FROM
    `{{ dataset }}.metrics` AS m
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    client_id,
    sample_id,
    normalized_channel
{% endfor -%}
