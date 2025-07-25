{{ header }}
CREATE TABLE IF NOT EXISTS
  `{{ project }}.glam_etl.{{ prefix }}__clients_histogram_aggregates_new_v1`(
    sample_id INT64,
    client_id STRING,
    ping_type STRING,
    os STRING,
    app_version INT64,
    app_build_id STRING,
    channel STRING,
    histogram_aggregates ARRAY<
      STRUCT<
        metric STRING,
        metric_type STRING,
        key STRING,
        agg_type STRING,
        value ARRAY<STRUCT<key STRING, value INT64>>
      >
    >
  )
PARTITION BY
  RANGE_BUCKET(sample_id, GENERATE_ARRAY(0, 100, 1))
CLUSTER BY
  app_version,
  channel,
  client_id
