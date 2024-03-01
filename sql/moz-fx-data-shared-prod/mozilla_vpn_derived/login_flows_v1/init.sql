CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.mozilla_vpn_derived.login_flows_v1`(
    flow_id STRING,
    flow_started TIMESTAMP,
    flow_completed TIMESTAMP,
    fxa_uids ARRAY<STRING>,
    viewed_email_first_page BOOL
  )
