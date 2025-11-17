SELECT
  submission_date,
  default_search_engine,
  SUM(search_count_all) AS searches,
  COUNT(DISTINCT(client_id)) AS users
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  default_search_engine
