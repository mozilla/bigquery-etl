CREATE TEMP FUNCTION udf_get_key(map ANY TYPE, k ANY TYPE) AS (
 (
   SELECT key_value.value
   FROM UNNEST(map) AS key_value
   WHERE key_value.key = k
   LIMIT 1
 )
);
--
SELECT
  submission_date,
  os,
  country,
  SUM(udf_get_key(histogram_parent_http_pageload_is_ssl, 0)) AS non_ssl_loads,
  SUM(udf_get_key(histogram_parent_http_pageload_is_ssl, 1)) AS ssl_loads,
  -- ratio of pings that have the probe
  COUNT(histogram_parent_http_pageload_is_ssl) / COUNT(*) AS reporting_ratio
FROM
  main_summary_v4
WHERE
  sample_id = 42
  AND normalized_channel = 'release'
  AND os IN ('Windows_NT', 'Darwin', 'Linux')
  AND app_name = 'Firefox'
  AND submission_date > DATE '2016-11-01'
  AND (submission_date = @submission_date
    OR @submission_date IS NULL)
GROUP BY
  submission_date,
  os,
  country
