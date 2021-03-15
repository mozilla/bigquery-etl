CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod`.telemetry_derived.desktop_funnel_new_profiles_v1(
    date DATE,
    country_name STRING,
    channel STRING,
    build_id STRING,
    os STRING,
    os_version NUMERIC,
    attribution_source STRING,
    distribution_id STRING,
    attribution_ua STRING,
    new_profiles INT64,
  )
PARTITION BY
  date
