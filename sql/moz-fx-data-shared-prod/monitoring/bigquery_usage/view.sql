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
  -- These cost estimates are calculated using BigQuery's list prices: https://cloud.google.com/bigquery/pricing
  CASE
    -- On-demand queries cost $6.25 per tebibyte.
    WHEN reservation_id IS NULL
      AND total_terabytes_billed IS NOT NULL
      THEN total_terabytes_billed * 6.25
    -- Default pipeline queries are free: https://docs.cloud.google.com/bigquery/docs/reservations-workload-management#default-pipeline
    WHEN reservation_id = 'default-pipeline'
      THEN 0
    -- Enterprise Edition queries using 1-year slot commitments cost $0.048 per slot hour.
    WHEN reservation_id LIKE '%.metadata-generation'
      OR reservation_id LIKE '%.shredder-all'
      OR reservation_id LIKE '%.shredder-telemetry-main'
      THEN (total_slot_ms / (60 * 60 * 1000)) * 0.048
    -- Enterprise Edition queries not using slot commitments cost $0.06 per slot hour.
    ELSE (total_slot_ms / (60 * 60 * 1000)) * 0.06
  END AS cost,
  total_slot_ms / (60 * 60 * 1000) AS total_slot_hours,
  ROW_NUMBER() OVER (PARTITION BY job_id, submission_date) AS referenced_table_number,
FROM
  `moz-fx-data-shared-prod.monitoring_derived.bigquery_usage_v2`
