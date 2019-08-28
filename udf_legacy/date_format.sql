/*

Shim for Presto/Athena's DATE_FORMAT to BigQuery's FORMAT_DATE

*/
CREATE TEMP FUNCTION
  udf_legacy_date_format(d DATE, f STRING)
  RETURNS STRING
  AS (
    FORMAT_DATE(f, d)
  );
