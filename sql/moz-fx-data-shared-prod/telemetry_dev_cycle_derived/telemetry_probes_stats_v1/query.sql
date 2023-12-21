WITH telemetry_probes_with_expiration AS (
  SELECT
    channel,
    probe,
    type,
    release_version,
    last_version,
    expiry_version,
    CASE
      WHEN expiry_version = "never"
        THEN last_version + 1
      ELSE IF(
          last_version + 1 < CAST(expiry_version AS int64),
          last_version + 1,
          CAST(expiry_version AS int64)
        )
    END AS expired_version_helper,
    first_added_date AS release_date
  FROM
    `telemetry_dev_cycle_external.telemetry_probes_stats_v1` AS probes
),
final AS (
  SELECT
    probes.channel,
    probes.probe,
    probes.type,
    probes.expiry_version,
    probes.release_version,
    release_dates.version AS expired_version,
    probes.release_date,
    CASE
      WHEN probes.expired_version_helper < 100
        THEN COALESCE(release_dates.publish_date, release_date)
      ELSE release_dates.publish_date
    END AS expired_date
  FROM
    telemetry_probes_with_expiration AS probes
  LEFT JOIN
    `telemetry_dev_cycle_derived.firefox_major_release_dates_v1` AS release_dates
  ON
    probes.channel = release_dates.channel
    AND probes.expired_version_helper = release_dates.version
)
SELECT
  *
FROM
  final
