CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.table_storage_trends`
AS
WITH dates_table AS (
  SELECT
    submission_date,
    DATE_SUB(submission_date, INTERVAL 3 day) AS date_3_days_ago,
    DATE_SUB(submission_date, INTERVAL 7 day) AS date_7_days_ago,
    DATE_SUB(submission_date, INTERVAL 14 day) AS date_14_days_ago
  FROM
    (
      SELECT
        MAX(SUBMISSION_DATE) AS submission_date
      FROM
        `moz-fx-data-shared-prod.monitoring_derived.table_storage_v1`
    ) latest_date_available
),
data_14_days_ago AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    total_rows AS total_rows_14_days_ago,
    total_partitions AS total_partitions_14_days_ago,
    total_logical_bytes AS total_logical_bytes_14_days_ago,
    active_logical_bytes AS active_logical_bytes_14_days_ago,
    long_term_logical_bytes AS long_term_logical_bytes_14_days_ago,
    total_physical_bytes AS total_physical_bytes_14_days_ago,
    time_travel_physical_bytes AS time_travel_physical_bytes_14_days_ago,
    active_physical_bytes AS active_physical_bytes_14_days_ago,
    long_term_physical_bytes AS long_term_physical_bytes_14_days_ago,
    current_physical_bytes AS current_physical_bytes_14_days_ago,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.table_storage_v1` ts
  JOIN
    dates_table d
    ON ts.submission_date = d.date_14_days_ago
),
data_7_days_ago AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    total_rows AS total_rows_7_days_ago,
    total_partitions AS total_partitions_7_days_ago,
    total_logical_bytes AS total_logical_bytes_7_days_ago,
    active_logical_bytes AS active_logical_bytes_7_days_ago,
    long_term_logical_bytes AS long_term_logical_bytes_7_days_ago,
    total_physical_bytes AS total_physical_bytes_7_days_ago,
    time_travel_physical_bytes AS time_travel_physical_bytes_7_days_ago,
    active_physical_bytes AS active_physical_bytes_7_days_ago,
    long_term_physical_bytes AS long_term_physical_bytes_7_days_ago,
    current_physical_bytes AS current_physical_bytes_7_days_ago
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.table_storage_v1` ts
  JOIN
    dates_table d
    ON ts.submission_date = d.date_7_days_ago
),
data_3_days_ago AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    total_rows AS total_rows_3_days_ago,
    total_partitions AS total_partitions_3_days_ago,
    total_logical_bytes AS total_logical_bytes_3_days_ago,
    active_logical_bytes AS active_logical_bytes_3_days_ago,
    long_term_logical_bytes AS long_term_logical_bytes_3_days_ago,
    total_physical_bytes AS total_physical_bytes_3_days_ago,
    time_travel_physical_bytes AS time_travel_physical_bytes_3_days_ago,
    active_physical_bytes AS active_physical_bytes_3_days_ago,
    long_term_physical_bytes AS long_term_physical_bytes_3_days_ago,
    current_physical_bytes AS current_physical_bytes_3_days_ago
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.table_storage_v1` ts
  JOIN
    dates_table d
    ON ts.submission_date = d.date_3_days_ago
),
latest AS (
  SELECT
    ts.submission_date,
    table_catalog,
    table_schema,
    table_name,
    total_rows,
    total_partitions,
    total_logical_bytes,
    active_logical_bytes,
    long_term_logical_bytes,
    total_physical_bytes,
    time_travel_physical_bytes,
    active_physical_bytes,
    long_term_physical_bytes,
    current_physical_bytes
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.table_storage_v1` ts
  JOIN
    dates_table d
    ON ts.submission_date = d.submission_date
)
SELECT
  l.submission_date,
  l.table_catalog,
  l.table_schema,
  l.table_name,
  l.total_rows,
  l.total_partitions,
  l.total_logical_bytes,
  l.active_logical_bytes,
  l.long_term_logical_bytes,
  l.total_physical_bytes,
  l.time_travel_physical_bytes,
  l.active_physical_bytes,
  l.long_term_physical_bytes,
  l.current_physical_bytes,
  d3.total_rows_3_days_ago,
  d3.total_partitions_3_days_ago,
  d3.total_logical_bytes_3_days_ago,
  d3.active_logical_bytes_3_days_ago,
  d3.long_term_logical_bytes_3_days_ago,
  d3.total_physical_bytes_3_days_ago,
  d3.time_travel_physical_bytes_3_days_ago,
  d3.active_physical_bytes_3_days_ago,
  d3.long_term_physical_bytes_3_days_ago,
  d3.current_physical_bytes_3_days_ago,
  w1.total_rows_7_days_ago,
  w1.total_partitions_7_days_ago,
  w1.total_logical_bytes_7_days_ago,
  w1.active_logical_bytes_7_days_ago,
  w1.long_term_logical_bytes_7_days_ago,
  w1.total_physical_bytes_7_days_ago,
  w1.time_travel_physical_bytes_7_days_ago,
  w1.active_physical_bytes_7_days_ago,
  w1.long_term_physical_bytes_7_days_ago,
  w1.current_physical_bytes_7_days_ago,
  w2.total_rows_14_days_ago,
  w2.total_partitions_14_days_ago,
  w2.total_logical_bytes_14_days_ago,
  w2.active_logical_bytes_14_days_ago,
  w2.long_term_logical_bytes_14_days_ago,
  w2.total_physical_bytes_14_days_ago,
  w2.time_travel_physical_bytes_14_days_ago,
  w2.active_physical_bytes_14_days_ago,
  w2.long_term_physical_bytes_14_days_ago,
  w2.current_physical_bytes_14_days_ago,
  l.total_partitions - d3.total_partitions_3_days_ago AS partition_change_last_3_days,
  l.total_partitions - w1.total_partitions_7_days_ago AS partition_change_last_7_days,
  l.total_partitions - w2.total_partitions_14_days_ago AS partition_change_last_14_days,
  l.total_rows - d3.total_rows_3_days_ago AS rows_change_last_3_days,
  l.total_rows - w1.total_rows_7_days_ago AS rows_change_last_7_days,
  l.total_rows - w2.total_rows_14_days_ago AS rows_change_last_14_days,
  l.current_physical_bytes - d3.current_physical_bytes_3_days_ago AS current_physical_bytes_change_last_3_days,
  l.current_physical_bytes - w1.current_physical_bytes_7_days_ago AS current_physical_bytes_change_last_7_days,
  l.current_physical_bytes - w2.current_physical_bytes_14_days_ago AS current_physical_bytes_change_last_14_days,
  l.total_logical_bytes - d3.total_logical_bytes_3_days_ago AS total_logical_bytes_change_last_3_days,
  l.total_logical_bytes - w1.total_logical_bytes_7_days_ago AS total_logical_bytes_change_last_7_days,
  l.total_logical_bytes - w2.total_logical_bytes_14_days_ago AS total_logical_bytes_change_last_14_days,
  l.active_logical_bytes - d3.active_logical_bytes_3_days_ago AS active_logical_bytes_change_last_3_days,
  l.active_logical_bytes - w1.active_logical_bytes_7_days_ago AS active_logical_bytes_change_last_7_days,
  l.active_logical_bytes - w2.active_logical_bytes_14_days_ago AS active_logical_bytes_change_last_14_days,
  l.long_term_logical_bytes - d3.long_term_logical_bytes_3_days_ago AS long_term_logical_bytes_change_last_3_days,
  l.long_term_logical_bytes - w1.long_term_logical_bytes_7_days_ago AS long_term_logical_bytes_change_last_7_days,
  l.long_term_logical_bytes - w2.long_term_logical_bytes_14_days_ago AS long_term_logical_bytes_change_last_14_days,
  l.total_physical_bytes - d3.total_physical_bytes_3_days_ago AS total_physical_bytes_change_last_3_days,
  l.total_physical_bytes - w1.total_physical_bytes_7_days_ago AS total_physical_bytes_change_last_7_days,
  l.total_physical_bytes - w2.total_physical_bytes_14_days_ago AS total_physical_bytes_change_last_14_days,
  l.time_travel_physical_bytes - d3.time_travel_physical_bytes_3_days_ago AS time_travel_physical_bytes_change_last_3_days,
  l.time_travel_physical_bytes - w1.time_travel_physical_bytes_7_days_ago AS time_travel_physical_bytes_change_last_7_days,
  l.time_travel_physical_bytes - w2.time_travel_physical_bytes_14_days_ago AS time_travel_physical_bytes_change_last_14_days
FROM
  latest AS l
LEFT JOIN
  data_3_days_ago AS d3
  USING (table_catalog, table_schema, table_name)
LEFT JOIN
  data_7_days_ago AS w1
  USING (table_catalog, table_schema, table_name)
LEFT JOIN
  data_14_days_ago w2
  USING (table_catalog, table_schema, table_name)
