WITH rank_per_client AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_timestamp DESC) AS rn,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.main_v5`
  WHERE
    DATE(submission_timestamp) >= @submission_date
    AND DATE(submission_timestamp) < DATE_ADD(@submission_date, INTERVAL 7 DAY)
),
latest_per_client_all AS (
  SELECT
    *
  FROM
    rank_per_client
  WHERE
    rn = 1
),
latest_per_client AS (
  SELECT
    environment.build.architecture AS browser_arch,
    COALESCE(environment.system.os.name, 'Other') AS os_name,
    COALESCE(
      CASE
        WHEN environment.system.os.name IN ('Linux', 'Darwin')
          THEN CONCAT(REGEXP_EXTRACT(environment.system.os.version, r"^[0-9]+"), '.x')
        WHEN environment.system.os.name = 'Windows_NT'
          AND environment.system.os.version = '10.0'
          AND environment.system.os.windows_build_number >= 20000
          THEN CONCAT(
              '10.0.',
              CAST(
                CAST(FLOOR(environment.system.os.windows_build_number / 10000) AS INT64) AS STRING
              ),
              'xxxx'
            )
        ELSE environment.system.os.version
      END,
      'Other'
    ) AS os_version,
    environment.system.memory_mb,
    COALESCE(environment.system.is_wow64, FALSE) AS is_wow64,
    IF(
      ARRAY_LENGTH(environment.system.gfx.adapters) > 0,
      environment.system.gfx.adapters[OFFSET(0)].vendor_id,
      NULL
    ) AS gfx0_vendor_id,
    IF(
      ARRAY_LENGTH(environment.system.gfx.adapters) > 0,
      environment.system.gfx.adapters[OFFSET(0)].device_id,
      NULL
    ) AS gfx0_device_id,
    IF(
      ARRAY_LENGTH(environment.system.gfx.monitors) > 0,
      environment.system.gfx.monitors[OFFSET(0)].screen_width,
      0
    ) AS screen_width,
    IF(
      ARRAY_LENGTH(environment.system.gfx.monitors) > 0,
      environment.system.gfx.monitors[OFFSET(0)].screen_height,
      0
    ) AS screen_height,
    environment.system.cpu.cores AS cpu_cores,
    IF(
      STARTS_WITH(environment.system.cpu.name, 'Apple '),
      'Apple',
      environment.system.cpu.vendor
    ) AS cpu_vendor,
    environment.system.cpu.speed_m_hz AS cpu_speed,
    'Shockwave Flash' IN (SELECT name FROM UNNEST(environment.addons.active_plugins)) AS has_flash
  FROM
    latest_per_client_all
),
transformed AS (
  SELECT
    browser_arch,
    CONCAT(os_name, '-', os_version) AS os,
    COALESCE(SAFE_CAST(ROUND(memory_mb / 1024.0) AS INT64), 0) AS memory_gb,
    is_wow64,
    gfx0_vendor_id,
    gfx0_device_id,
    CONCAT(CAST(screen_width AS STRING), 'x', CAST(screen_height AS STRING)) AS resolution,
    cpu_cores,
    cpu_vendor,
    -- FORMAT with %t instead of cast to retain .0
    COALESCE(FORMAT('%t', ROUND(cpu_speed / 1000, 1)), "Other") AS cpu_speed,
    has_flash
  FROM
    latest_per_client
),
by_dimensions AS (
  SELECT
    *,
    COUNT(*) AS client_count
  FROM
    transformed
  GROUP BY
    browser_arch,
    os,
    memory_gb,
    is_wow64,
    gfx0_vendor_id,
    gfx0_device_id,
    resolution,
    cpu_cores,
    cpu_vendor,
    cpu_speed,
    has_flash
)
SELECT
  @submission_date AS date_from,
  DATE_ADD(@submission_date, INTERVAL 7 DAY) AS date_to,
  *,
FROM
  by_dimensions
