SELECT
  DATE(submission_timestamp) AS submission_date,
  environment.system.os.name AS os,
  metadata.geo.country,
  SUM(
    mozfun.map.get_key(mozfun.hist.extract(payload.histograms.http_pageload_is_ssl).values, 0)
  ) AS non_ssl_loads,
  SUM(
    mozfun.map.get_key(mozfun.hist.extract(payload.histograms.http_pageload_is_ssl).values, 1)
  ) AS ssl_loads,
  -- ratio of pings that have the probe
  COUNT(payload.histograms.http_pageload_is_ssl) / COUNT(*) AS reporting_ratio
FROM
  telemetry.main
WHERE
  sample_id = 42
  AND normalized_channel = 'release'
  AND environment.system.os.name IN ('Windows_NT', 'Darwin', 'Linux')
  AND application.name = 'Firefox'
  AND DATE(submission_timestamp) > DATE '2016-11-01'
  AND (DATE(submission_timestamp) = @submission_date OR @submission_date IS NULL)
GROUP BY
  submission_date,
  os,
  country
