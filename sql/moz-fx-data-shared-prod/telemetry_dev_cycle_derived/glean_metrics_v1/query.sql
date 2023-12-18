WITH glean_metrics AS (
  SELECT
    product,
    metric,
    type,
    first_seen_date AS release_date,
    last_seen_date AS last_date,
    CASE
      when expires = "never" then NULL
      when REGEXP_CONTAINS(expires, r'[0-9]{4}-[0-9]{2}-[0-9]{2}') THEN CAST(expires AS DATE)
      else NULL
    END AS expiry_date,
    expires
  FROM `leli-sandbox.telemetry_dev_cycle.glean_metrics_external_v1` #todo change that
),
firefox_releases AS (
  SELECT
    CAST(REGEXP_EXTRACT (build.target.version, r"^\d+")AS INT64) AS version,
    build.target.channel,
    DATE(MIN(build.download.date)) AS publish_date,
  FROM
    mozdata.telemetry.buildhub2
  WHERE
    build.target.channel in ("release", "beta", "nightly")
  GROUP BY
    version,
    channel
),

final AS (
  SELECT 1
)

SELECT * FROM final