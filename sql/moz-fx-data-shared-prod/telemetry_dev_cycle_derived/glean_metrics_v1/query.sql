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
      WHEN REGEXP_CONTAINS(glean.glean_app, r'beta')
        THEN releases.publish_date_beta
      WHEN REGEXP_CONTAINS(glean.glean_app, r"nightly")
        THEN releases.publish_date_nightly
      ELSE releases.publish_date_release
    END AS expiry_date,
  FROM
    `telemetry_dev_cycle_derived.glean_metrics_external_v1` AS glean
  LEFT JOIN
    `telemetry_dev_cycle_derived.firefox_major_releases_dates_v1` AS releases
  ON
    glean.expires = CAST(releases.version AS STRING)
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
