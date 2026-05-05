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
),
default_percent_by_os_version AS (
  SELECT
    submission_date_s3,
    os_version,
    ROUND(AVG(IF(is_default_browser, 1, 0)) * 100, 1) AS default_percent,
    SUM(ROUND(profile_age_in_days)) / COUNT(DISTINCT(client_id)) AS average_profile_age,
    SUM(sessions_started_on_this_day) AS nbr_sessions,
    COUNT(DISTINCT(client_id)) AS total_nbr_users,
    COUNT(
      DISTINCT(CASE WHEN profile_age_in_days <= 7 THEN client_id ELSE NULL END)
    ) AS nbr_users_profile_age_less_than_7,
    COUNT(
      DISTINCT(CASE WHEN profile_age_in_days BETWEEN 8 AND 365 THEN client_id ELSE NULL END)
    ) AS nbr_users_profile_age_between_8_and_365,
    COUNT(
      DISTINCT(CASE WHEN profile_age_in_days > 365 THEN client_id ELSE NULL END)
    ) AS nbr_users_profile_age_over_365,
    SUM(scalar_parent_browser_engagement_total_uri_count_sum) AS uris,
    COUNTIF(profile_age_in_days = 0) AS new_profiles,
    SUM(ROUND(scalar_parent_browser_engagement_unique_domains_count_mean)) AS domains
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date_s3 = @submission_date
    AND os = 'Windows_NT'
    AND sample_id = 42
    AND os_version IN ('6.1', '6.2', '6.3', '10.0')
  GROUP BY
    submission_date_s3,
    os_version
)
SELECT
  COALESCE(
    COALESCE(spu.submission_date_s3, sshpu.submission_date_s3),
    ahpu.submission_date_s3
  ) AS submission_date,
  COALESCE(COALESCE(spu.os_version, sshpu.os_version), ahpu.os_version) AS os_version,
  spu.searches_per_user_ratio,
  sshpu.subsession_hours_per_user_ratio,
  ahpu.active_hours_per_user_ratio,
  dflt.default_percent,
  dflt.average_profile_age,
  dflt.nbr_users_profile_age_less_than_7 / dflt.total_nbr_users AS pct_users_profile_age_less_than_7_days,
  dflt.nbr_users_profile_age_between_8_and_365 / dflt.total_nbr_users AS pct_users_profile_age_8_to_365_days,
  dflt.nbr_users_profile_age_over_365 / dflt.total_nbr_users AS pct_users_profile_age_over_365_days,
  dflt.nbr_sessions / dflt.total_nbr_users AS sessions_per_user,
  dflt.uris / dflt.total_nbr_users AS uris_per_user,
  dflt.new_profiles / dflt.total_nbr_users AS new_profile_ratio,
  dflt.domains / dflt.total_nbr_users AS domains_per_user
FROM
  searches_per_user_by_os_and_date AS spu
FULL OUTER JOIN
  subsession_hours_per_user AS sshpu
  ON spu.os_version = sshpu.os_version
FULL OUTER JOIN
  active_hours_per_user AS ahpu
  ON COALESCE(spu.os_version, sshpu.os_version) = ahpu.os_version
FULL OUTER JOIN
  default_percent_by_os_version AS dflt
  ON COALESCE(COALESCE(spu.os_version, sshpu.os_version), ahpu.os_version) = dflt.os_version
