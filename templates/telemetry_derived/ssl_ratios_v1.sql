SELECT
  submission_date,
  os,
  country,
  SUM(udf_get_key(histogram_parent_http_pageload_is_ssl, 0)) AS non_ssl_loads,
  SUM(udf_get_key(histogram_parent_http_pageload_is_ssl, 1)) AS ssl_loads,
  -- ratio of pings that have the probe
  COUNT(histogram_parent_http_pageload_is_ssl) / COUNT(*) AS reporting_ratio
FROM
  telemetry.main_summary_v4
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
