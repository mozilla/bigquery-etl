WITH searches_per_user_by_os_version_and_date_staging AS (
  SELECT
    submission_date_s3,
    os_version,
    SUM(search_count_all) AS searches,
    COUNT(DISTINCT client_id) AS users,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date_s3 = @submission_date
    AND app_name = 'Firefox'
    AND sample_id = 42
    AND search_count_all < 10000
    AND os = 'Windows_NT'
    AND os_version IN ('6.1', '6.2', '6.3', '10.0')
  GROUP BY
    submission_date_s3,
    os_version
),
searches_per_user_by_os_and_date AS (
  SELECT
    submission_date_s3,
    os_version,
    searches / users AS searches_per_user_ratio,
  FROM
    searches_per_user_by_os_version_and_date_staging
),
subsession_hours_per_user_staging AS (
  SELECT
    submission_date_s3,
    os_version,
    SUM(subsession_hours_sum) AS `hours`,
    COUNT(DISTINCT client_id) AS users,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date_s3 = @submission_date
    AND app_name = 'Firefox'
    AND sample_id = 42
    AND subsession_hours_sum < 24
    AND os = 'Windows_NT'
    AND os_version IN ('6.1', '6.2', '6.3', '10.0')
  GROUP BY
    submission_date_s3,
    os_version
),
subsession_hours_per_user AS (
  SELECT
    submission_date_s3,
    os_version,
    `hours` / users AS subsession_hours_per_user_ratio
  FROM
    subsession_hours_per_user_staging
),
active_hours_per_user_staging AS (
  SELECT
    submission_date_s3,
    os_version,
    SUM(active_hours_sum) AS `hours`,
    COUNT(DISTINCT(client_id)) AS users,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date_s3 = @submission_date
    AND app_name = 'Firefox'
    AND sample_id = 42
    AND os = 'Windows_NT'
    AND os_version IN ('6.1', '6.2', '6.3', '10.0')
  GROUP BY
    submission_date_s3,
    os_version
),
active_hours_per_user AS (
  SELECT
    submission_date_s3,
    os_version,
    `hours` / users AS active_hours_per_user_ratio
  FROM
    active_hours_per_user_staging
)
SELECT
  COALESCE(
    COALESCE(spu.submission_date_s3, sshpu.submission_date_s3),
    ahpu.submission_date_s3
  ) AS submission_date,
  COALESCE(COALESCE(spu.os_version, sshpu.os_version), ahpu.os_version) AS os_version,
  spu.searches_per_user_ratio,
  sshpu.subsession_hours_per_user_ratio,
  ahpu.active_hours_per_user_ratio
FROM
  searches_per_user_by_os_and_date AS spu
FULL OUTER JOIN
  subsession_hours_per_user AS sshpu
  ON spu.os_version = sshpu.os_version
FULL OUTER JOIN
  active_hours_per_user AS ahpu
  ON COALESCE(spu.os_version, sshpu.os_version) = ahpu.os_version
