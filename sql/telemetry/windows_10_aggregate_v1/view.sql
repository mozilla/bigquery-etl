CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.windows_10_aggregate_v1` AS
WITH
  aggregated AS (
  SELECT
    os AS name,
    os_version AS version,
    windows_build_number AS build_number,
    CAST(windows_ubr AS STRING) AS ubr,
  IF
    ((windows_build_number <= 10240),
      '1507',
    IF
      ((windows_build_number <= 10586),
        '1511',
      IF
        ((windows_build_number <= 14393),
          '1607',
        IF
          ((windows_build_number <= 15063),
            '1703',
          IF
            ((windows_build_number <= 16299),
              '1709',
            IF
              ((windows_build_number <= 17134),
                '1803',
              IF
                ((windows_build_number <= 17763),
                  '1809',
                IF
                  ((windows_build_number <= 18362),
                    '1903',
                  IF
                    ((windows_build_number > 18362),
                      'Insider',
                      NULL))))))))) build_group,
    SPLIT(app_version, ".")[
  OFFSET
    (0)] AS ff_build_version,
    normalized_channel,
    COUNT(1) count
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
    AND os = 'Windows_NT'
    AND STARTS_WITH(os_version, '10')
    AND CAST(SPLIT(app_version, ".")[
    OFFSET
      (0)] AS INT64) >= 47
    AND sample_id=42
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7 )
SELECT
  *
FROM (aggregated
  CROSS JOIN (
    SELECT
      SUM(count) total_obs
    FROM
      aggregated ) AS total)
