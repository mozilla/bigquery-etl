
#fail
WITH rows_per_partition AS (
  SELECT
    PARSE_DATE("%Y%m%d", partition_id) AS table_partition,
    total_rows
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    table_name = "active_users_aggregates_v1"
    AND partition_id != "__NULL__"
    AND PARSE_DATE("%Y%m%d", partition_id)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 7 + 1 DAY)
    AND DATE(@submission_date)
),
row_counts_current_and_historic AS (
  SELECT
    SUM(IF(table_partition = @submission_date, total_rows, NULL)) AS current_partition_row_count,
    AVG(
      IF(table_partition < @submission_date, total_rows, NULL)
    ) AS historic_partition_avg_row_count,
  FROM
    rows_per_partition
),
row_count_boundaries AS (
  SELECT
    CAST(current_partition_row_count AS INT64) AS current_partition_row_count,
    CAST(historic_partition_avg_row_count * (1 - 5 / 100) AS INT64) AS lower_bound,
    CAST(historic_partition_avg_row_count * (1 + 5 / 100) AS INT64) AS upper_bound
  FROM
    row_counts_current_and_historic
)
SELECT
  IF(
    current_partition_row_count NOT
    BETWEEN lower_bound
    AND upper_bound,
    ERROR(
      CONCAT(
        "The row count for partition ",
        @submission_date,
        " is outside of the expected boundaries. ",
        "Row count for the current_partition: ",
        current_partition_row_count,
        ". Expected range: ",
        lower_bound,
        " - ",
        upper_bound
      )
    ),
    NULL
  )
FROM
  row_count_boundaries;
