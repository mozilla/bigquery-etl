SELECT
  submission_date,
  os,
  country,
  non_ssl_loads_v1 AS non_ssl_loads,
  ssl_loads_v1 AS ssl_loads,
  http_pageload_is_ssl_ratio_v1 AS reporting_ratio
FROM
  (
    WITH main AS (
      SELECT
        submission_date AS submission_date,
        environment.system.os.name AS os,
        metadata.geo.country AS country,
        SUM(
          mozfun.map.get_key(mozfun.hist.extract(payload.histograms.http_pageload_is_ssl).values, 0)
        ) AS non_ssl_loads_v1,
        SUM(
          mozfun.map.get_key(mozfun.hist.extract(payload.histograms.http_pageload_is_ssl).values, 1)
        ) AS ssl_loads_v1,
        COUNT(payload.histograms.http_pageload_is_ssl) / COUNT(*) AS http_pageload_is_ssl_ratio_v1,
      FROM
        (
          SELECT
            *,
            DATE(submission_timestamp) AS submission_date,
            environment.experiments
          FROM
            `moz-fx-data-shared-prod.telemetry_stable.main_v5`
        )
      WHERE
        sample_id = 42
        AND normalized_channel = 'release'
        AND environment.system.os.name IN ('Windows_NT', 'Darwin', 'Linux')
        AND application.name = 'Firefox'
        AND DATE(submission_timestamp) > DATE '2016-11-01'
        AND (DATE(submission_timestamp) = @submission_date OR @submission_date IS NULL)
      GROUP BY
        os,
        country,
        submission_date
    )
    SELECT
      main.submission_date,
      main.os AS os,
      main.country AS country,
      non_ssl_loads_v1,
      ssl_loads_v1,
      http_pageload_is_ssl_ratio_v1,
    FROM
      main
  )
