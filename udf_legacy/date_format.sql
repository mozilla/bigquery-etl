/*

Shim for Presto/Athena's DATE_FORMAT to BigQuery's FORMAT_DATE

Note that the format strings for FORMAT_DATE vs DATE_FORMAT are not
identical, but usages found in existing scheduled queries had format
strings that were portable between the two

*/
CREATE TEMP FUNCTION
  udf_legacy_date_format(d DATE, f STRING)
  RETURNS STRING
  AS (
    FORMAT_DATE(f, d)
  );
