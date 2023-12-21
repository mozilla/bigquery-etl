WITH glean_app_with_parsed_expiry_date AS (
  SELECT
    glean.glean_app,
    glean.metric,
    glean.type,
    glean.first_seen_date AS release_date,
    glean.last_seen_date AS last_date,
    glean.expires,
    CASE
      WHEN glean.expires = "never"
        THEN NULL
      WHEN REGEXP_CONTAINS(glean.expires, r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
        THEN CAST(glean.expires AS DATE)
      ELSE releases.publish_date
    END AS expiry_date,
  FROM
    `telemetry_dev_cycle_external.glean_metrics_stats_v1` AS glean
  LEFT JOIN
    `telemetry_dev_cycle_derived.firefox_major_release_dates_v1` AS releases
  ON
    glean.expires = CAST(releases.version AS STRING)
    AND COALESCE(
      REGEXP_EXTRACT(glean.glean_app, r"beta"),
      REGEXP_EXTRACT(glean.glean_app, r"nightly"),
      "release"
    ) = releases.channel
),
final AS (
  SELECT
    glean_app,
    metric,
    type,
    release_date,
    last_date,
    expires,
    CASE
      WHEN expiry_date IS NULL
        AND DATE_DIFF(CURRENT_DATE(), last_date, day) < 2
        THEN NULL
      WHEN expiry_date IS NULL
        THEN last_date
      ELSE IF(expiry_date < last_date, expiry_date, last_date)
    END AS expired_date
  FROM
    glean_app_with_parsed_expiry_date
)
SELECT
  *
FROM
  final
