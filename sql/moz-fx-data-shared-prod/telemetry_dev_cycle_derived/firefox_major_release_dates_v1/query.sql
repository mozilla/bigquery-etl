WITH firefox_releases AS (
  SELECT
    CAST(REGEXP_EXTRACT(build.target.version, r"^\d+") AS INT64) AS version,
    DATE(
      MIN(IF(build.target.channel = "release", build.download.date, NULL))
    ) AS publish_date_release,
    DATE(MIN(IF(build.target.channel = "beta", build.download.date, NULL))) AS publish_date_beta,
    DATE(
      MIN(IF(build.target.channel = "nightly", build.download.date, NULL))
    ) AS publish_date_nightly,
  FROM
    `mozdata.telemetry.buildhub2`
  WHERE
    build.target.channel IN ("release", "beta", "nightly")
  GROUP BY
    version
)
SELECT
  *
FROM
  firefox_releases
