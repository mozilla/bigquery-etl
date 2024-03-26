CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.mozilla_vpn_derived.fxa_attribution_v1`(
    flow_id STRING,
    flow_started TIMESTAMP,
    fxa_uids ARRAY<STRING>,
    attribution STRUCT<
      `timestamp` TIMESTAMP,
      entrypoint_experiment STRING,
      entrypoint_variation STRING,
      utm_campaign STRING,
      utm_content STRING,
      utm_medium STRING,
      utm_source STRING,
      utm_term STRING
    >
  )
