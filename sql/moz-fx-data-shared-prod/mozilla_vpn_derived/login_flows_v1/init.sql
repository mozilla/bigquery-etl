CREATE TABLE IF NOT EXISTS
  login_flows_v1(
    flow_id STRING,
    flow_started TIMESTAMP,
    flow_completed TIMESTAMP,
    fxa_uids ARRAY<STRING>,
    viewed_email_first_page BOOL
  )
