{{ header }}
CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.{{ destination_table }}`(
    {{ attributes }},
    scalar_aggregates {{ user_data_type }}
  )
{{ partition_clause }}
CLUSTER BY
  app_version,
  channel,
  client_id
