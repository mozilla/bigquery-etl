SELECT
  submission_date,
  os,
  country,
  non_ssl_loads_v1 AS non_ssl_loads,
  ssl_loads_v1 AS ssl_loads,
  http_pageload_is_ssl_ratio_v1 AS reporting_ratio
FROM
  {{
  metrics.calculate(
    metrics=["non_ssl_loads_v1", "ssl_loads_v1", "http_pageload_is_ssl_ratio_v1"],
    platform="firefox_desktop",
    group_by={"os": "environment.system.os.name", "country": "metadata.geo.country"},
    where="""
      sample_id = 42
      AND normalized_channel = 'release'
      AND environment.system.os.name IN ('Windows_NT', 'Darwin', 'Linux')
      AND application.name = 'Firefox'
      AND DATE(submission_timestamp) > DATE '2016-11-01'
      AND (DATE(submission_timestamp) = @submission_date OR @submission_date IS NULL)
    """,
    group_by_client_id=False
  )
}}
