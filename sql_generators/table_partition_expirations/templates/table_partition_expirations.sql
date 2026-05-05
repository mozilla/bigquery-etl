WITH
  {% for dataset_id in datasets %}
    first_partition_{{ dataset_id }} AS (
      SELECT
        table_catalog,
        table_schema,
        table_name,
        PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
        total_rows AS first_partition_row_count,
      FROM
        `moz-fx-data-shared-prod.{{ dataset_id }}.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        partition_id != "__NULL__"
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
    ),
    first_non_empty_partition_{{ dataset_id }} AS (
      SELECT
        table_name,
        PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
      FROM
        `moz-fx-data-shared-prod.{{ dataset_id }}.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        partition_id != "__NULL__"
        AND total_rows > 0
      GROUP BY
        table_name
    ),
  {% endfor %}
current_partitions AS (
  {% for dataset_id in datasets %}
    SELECT
      {% raw %}
        {% if is_init() %}
          CURRENT_DATE() - 1
        {% else %}
          DATE(@submission_date)
        {% endif %}
      {% endraw %}
      AS run_date,
      table_catalog AS project_id,
      table_schema AS dataset_id,
      table_name AS table_id,
      first_partition_current,
      first_non_empty_partition_current,
      first_partition_row_count,
    FROM
      first_partition_{{ dataset_id }}
    LEFT JOIN
      first_non_empty_partition_{{ dataset_id }}
    USING
      (table_name)
    {% if not loop.last %}
      UNION ALL
    {% endif %}
  {% endfor %}
),
partition_expirations AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE
    option_name = "partition_expiration_days"
),
partition_stats AS (
  SELECT
    current_partitions.run_date,
    current_partitions.project_id,
    current_partitions.dataset_id,
    current_partitions.table_id,
    COALESCE(
      previous.first_partition_historical,
      current_partitions.first_partition_current
    ) AS first_partition_historical,
    current_partitions.first_partition_current,
    {% raw %}
      {% if is_init() %}
        -- can't know whether the table had data in the past
        -- if data is already being dropped, so assume it did
        IF(
          DATE_DIFF(
            current_partitions.run_date,
            current_partitions.first_partition_current,
            DAY
          ) >= CAST(option_value AS FLOAT64) - 2,
          current_partitions.first_partition_current,
          current_partitions.first_non_empty_partition_current
        )
      {% else %}
        COALESCE(
          previous.first_non_empty_partition_historical,
          current_partitions.first_non_empty_partition_current
        )
      {% endif %}
    {% endraw %}
    AS first_non_empty_partition_historical,
    current_partitions.first_non_empty_partition_current,
    current_partitions.first_partition_row_count,
    CAST(CAST(option_value AS FLOAT64) AS INT) AS partition_expiration_days,
    previous.partition_expiration_days AS previous_partition_expiration_days,
  FROM
    current_partitions
  LEFT JOIN
    `moz-fx-data-shared-prod.monitoring_derived.table_partition_expirations_v1` AS previous
    ON current_partitions.project_id = previous.project_id
    AND current_partitions.dataset_id = previous.dataset_id
    AND current_partitions.table_id = previous.table_id
    AND current_partitions.run_date = previous.run_date + 1
  LEFT JOIN
    partition_expirations
    ON table_catalog = current_partitions.project_id
    AND table_schema = current_partitions.dataset_id
    AND table_name = current_partitions.table_id
)

SELECT
  *,
  -- If there was data in the past, then first partition is the next deletion.
  -- Otherwise, next deletion is the first non-empty partition.
  DATE_ADD(
    IF(
      COALESCE(first_non_empty_partition_historical, '9999-12-31') <= first_partition_current,
      first_partition_current,
      first_non_empty_partition_current
    ),
    INTERVAL partition_expiration_days DAY
  ) AS next_deletion_date,
  COALESCE(previous_partition_expiration_days, -1) != COALESCE(partition_expiration_days, -1) AS expiration_changed,
FROM
  partition_stats
