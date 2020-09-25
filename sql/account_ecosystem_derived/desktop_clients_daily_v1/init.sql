CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.account_ecosystem_derived.desktop_clients_daily_v1`(
    submission_date DATE,
    ecosystem_client_id_hash STRING,
    duration_sum INT64,
    normalized_channel STRING
  )
PARTITION BY
  submission_date
CLUSTER BY
  normalized_channel
