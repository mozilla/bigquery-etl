CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.bigquery_usage`
AS
SELECT
  *,
  CASE
    WHEN user_email LIKE "%looker%"
      THEN "looker"
    WHEN user_email LIKE "%redash%"
      THEN "redash"
    WHEN user_email LIKE "%pocket%"
      THEN "pocket"
    WHEN user_email LIKE "%grafana%"
      THEN "grafana"
    WHEN user_email LIKE "%amo%"
      THEN "amo"
    WHEN user_email LIKE "%search-terms%"
      THEN "search-terms"
    WHEN user_email LIKE "%airflow%"
      THEN "airflow"
    WHEN user_email LIKE "%bigeye%"
      THEN "bigeye"
    WHEN ENDS_WITH(user_email, "mozilla.com")
      THEN "individual"
    WHEN ENDS_WITH(user_email, "mozillafoundation.org")
      THEN "individual"
    ELSE SPLIT(user_email, '@')[SAFE_OFFSET(0)]
  END AS user_type,
  'https://sql.telemetry.mozilla.org/queries/' || query_id || '/source' AS query_url,
  username = "Scheduled" AS is_scheduled,
  (total_slot_ms * 0.06) / (60 * 60 * 1000) AS cost,
  total_slot_ms / (60 * 60 * 1000) AS total_slot_hours,
  ROW_NUMBER() OVER (PARTITION BY job_id, submission_date) AS referenced_table_number,
  labels,
FROM
  `moz-fx-data-shared-prod.monitoring_derived.bigquery_usage_v2`
