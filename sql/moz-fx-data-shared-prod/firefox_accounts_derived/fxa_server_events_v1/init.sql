CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_server_events_v1`(
    `timestamp` TIMESTAMP,
    fxa_log STRING
  )
PARTITION BY
  DATE(`timestamp`)
CLUSTER BY
  fxa_log
