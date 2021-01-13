CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod`.telmetry_derived.desktop_funnel_activation_v1(
    date DATE,
    country_name STRING,
    channel STRING,
    build_id STRING,
    os STRING,
    os_version NUMERIC,
    attribution_source STRING,
    distribution_id STRING,
    attribution_ua STRING,
    num_activated INT64,
  )
PARTITION BY
  date
