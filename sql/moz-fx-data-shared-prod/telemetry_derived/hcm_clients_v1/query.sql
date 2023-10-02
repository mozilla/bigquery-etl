SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_channel AS channel,
  environment.system.os.name AS os,
  COUNT(DISTINCT client_id) AS num_clients,
FROM
  `moz-fx-data-shared-prod`.telemetry_stable.main_v5
CROSS JOIN
  UNNEST(payload.processes.parent.keyed_scalars.a11y_theme)
WHERE
  {% if is_init() %}
    DATE(submission_timestamp) >= '2021-01-01'
  {% else %}
    DATE(submission_timestamp) = @submission_date
  {% endif %}
  AND payload.processes.parent.keyed_scalars.a11y_theme IS NOT NULL
  AND ARRAY_LENGTH(payload.processes.parent.keyed_scalars.a11y_theme) > 0
  AND value IS TRUE
  AND key != "always"
GROUP BY
  submission_date,
  channel,
  os
