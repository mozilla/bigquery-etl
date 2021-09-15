CREATE TABLE IF NOT EXISTS
  `{{gcp_project}}.{{dataset}}.{{slug}}_scalar` (
    submission_date DATE,
    client_id STRING,
    build_id STRING,
    branch STRING,
    {% for dimension in dimensions %}
      {{ dimension.name }} STRING,
    {% endfor %}
    name STRING,
    agg_type STRING,
    value FLOAT64
  )
PARTITION BY submission_date
CLUSTER BY
    build_id
OPTIONS (
    require_partition_filter = TRUE,
    partition_expiration_days = 5
)
