SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_country_code,
  count(DISTINCT client_id) AS nbr_distinct_clients
FROM
  `moz-fx-data-shared-prod.telemetry.main`
CROSS JOIN
  UNNEST(payload.processes.parent.keyed_scalars.browser_ui_interaction_nav_bar) AS a
WHERE
  DATE(submission_timestamp) = @submission_date
  AND a.key LIKE('%reload%')
  AND normalized_channel = 'release'
GROUP BY
  DATE(submission_timestamp),
  normalized_country_code
