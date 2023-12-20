WITH firefox_releases AS (
  SELECT
    CAST(REGEXP_EXTRACT(build.target.version, r"^\d+") AS INT64) AS version,
    build.target.channel,
    DATE(MIN(build.download.date)) AS publish_date
  FROM
    `mozdata.telemetry.buildhub2`
  WHERE
    build.target.channel IN ("release", "beta", "nightly")
  GROUP BY
    version,
    channel
)
SELECT
  *
FROM
  firefox_releases
