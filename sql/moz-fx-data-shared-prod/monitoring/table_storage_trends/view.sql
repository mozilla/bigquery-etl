CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.table_storage_trends`
AS
WITH dates_table AS (
  SELECT
    submission_date,
    DATE_SUB(submission_date, INTERVAL 7 day) AS date_7_days_ago,
    DATE_SUB(submission_date, INTERVAL 14 day) AS date_14_days_ago,
    DATE_SUB(submission_date, INTERVAL 21 day) AS date_21_days_ago
  FROM
    (
      SELECT
        MAX(SUBMISSION_DATE) AS submission_date
      FROM
        `moz-fx-data-shared-prod.monitoring_derived.table_storage_v1`
    ) latest_date_available
),
data_21_days_ago AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    total_rows AS total_rows_21_days_ago,
    total_partitions AS total_partitions_21_days_ago,
    active_physical_bytes AS active_physical_bytes_21_days_ago,
    long_term_physical_bytes AS long_term_physical_bytes_21_days_ago
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.table_storage_v1` ts
  JOIN
    dates_table d
    ON ts.submission_date = d.date_21_days_ago
),
data_14_days_ago AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    total_rows AS total_rows_14_days_ago,
    total_partitions AS total_partitions_14_days_ago,
    active_physical_bytes AS active_physical_bytes_14_days_ago,
    long_term_physical_bytes AS long_term_physical_bytes_14_days_ago
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
    active_physical_bytes AS active_physical_bytes_7_days_ago,
    long_term_physical_bytes AS long_term_physical_bytes_7_days_ago
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.table_storage_v1` ts
  JOIN
    dates_table d
    ON ts.submission_date = d.date_14_days_ago
),
latest AS (
  SELECT
    submission_date,
    table_catalog,
    table_schema,
    table_name,
    total_rows,
    total_partitions,
    active_physical_bytes,
    long_term_physical_bytes
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.table_storage_v1`
)
SELECT
--latest
  l.submission_date,
  l.table_catalog,
  l.table_schema,
  l.table_name,
  l.total_rows,
  l.total_partitions,
  l.active_physical_bytes,
  l.long_term_physical_bytes,
--w1
  w1.total_rows_7_days_ago,
  w1.total_partitions_7_days_ago,
  w1.active_physical_bytes_7_days_ago,
  w1.long_term_physical_bytes_7_days_ago,
--w2
  w2.total_rows_14_days_ago,
  w2.total_partitions_14_days_ago,
  w2.active_physical_bytes_14_days_ago,
  w2.long_term_physical_bytes_14_days_ago,
--w3
  w3.total_rows_21_days_ago,
  w3.total_partitions_21_days_ago,
  w3.active_physical_bytes_21_days_ago,
  w3.long_term_physical_bytes_21_days_ago
FROM
  latest AS l
LEFT JOIN
  data_7_days_ago AS w1
  USING (table_catalog, table_schema, table_name)
LEFT JOIN
  data_14_days_ago w2
  USING (table_catalog, table_schema, table_name)
LEFT JOIN
  data_21_days_ago AS w3
  USING (table_catalog, table_schema, table_name)
