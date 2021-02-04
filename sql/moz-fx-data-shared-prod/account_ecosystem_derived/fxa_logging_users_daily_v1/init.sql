CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.account_ecosystem_derived.fxa_logging_users_daily_v1`(
    submission_date DATE,
    canonical_id STRING,
    ecosystem_user_id STRING,
    oauth_client_id STRING,
    event_count INT64,
    country_name STRING,
    country_code STRING
  )
PARTITION BY
  submission_date
CLUSTER BY
  ecosystem_user_id
