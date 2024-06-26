SELECT
  DATE(submission_timestamp) AS date,
  client_info.app_display_version AS mozregression_version,
  metrics.string.usage_variant AS mozregression_variant,
  metrics.string.usage_app AS app_used,
  normalized_os AS os,
  mozfun.norm.truncate_version(normalized_os_version, "minor") AS os_version,
  COUNT(DISTINCT(client_info.client_id)) AS distinct_clients,
  COUNT(*) AS total_uses
FROM
  `moz-fx-data-shared-prod`.org_mozilla_mozregression.usage
WHERE
  {% if is_init() %}
    DATE(submission_timestamp) > '2020-04-01'
  {% else %}
    DATE(submission_timestamp) = @submission_date
  {% endif %}
  AND client_info.app_display_version NOT LIKE '%.dev%'
GROUP BY
  date,
  mozregression_version,
  mozregression_variant,
  app_used,
  os,
  os_version;
