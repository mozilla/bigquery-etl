{{ header }}
CREATE TABLE IF NOT EXISTS
  `{{ project }}.{{ destination_table }}`(
    {{ attributes_type }},
    scalar_aggregates {{ user_data_type }}
  )
PARTITION BY
  RANGE_BUCKET(app_version, GENERATE_ARRAY(0, 100, 1))
CLUSTER BY
  app_version,
  channel,
  client_id
