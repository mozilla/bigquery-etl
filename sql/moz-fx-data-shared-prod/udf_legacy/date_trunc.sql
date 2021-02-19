/*
  Translates Presto/Athena date_trunc parameters to bq date_trunc.
  Presto/Athena version was able to handle both DATE and DATETIME types so
  some migrated invocations will probably fail.
*/
CREATE TEMP FUNCTION
  udf_legacy_date_trunc(part STRING, d DATE)
  AS (
    CASE
    WHEN lower(part) = 'day' THEN DATE_TRUNC(d, DAY)
    WHEN lower(part) = 'week' THEN DATE_TRUNC(d, WEEK)
    WHEN lower(part) = 'month' THEN DATE_TRUNC(d, MONTH)
    ELSE ERROR('This function is a legacy compatibility method and should not be used in new queries. Use the BigQuery built-in DATE_TRUNC instead')
    END
  );
