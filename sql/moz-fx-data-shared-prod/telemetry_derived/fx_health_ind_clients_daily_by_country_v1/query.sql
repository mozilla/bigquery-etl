WITH searches_per_user_by_country_and_date_staging AS (
  SELECT
    submission_date_s3,
    country,
    SUM(search_count_all) AS searches,
    COUNT(DISTINCT client_id) AS users,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date_s3 = @submission_date
    AND app_name = 'Firefox'
    AND sample_id = 42
    AND search_count_all < 10000
  GROUP BY
    submission_date_s3,
    country
),
searches_per_user_by_country_and_date AS (
  SELECT
    submission_date_s3,
    country,
    searches / users AS searches_per_user_ratio,
  FROM
    searches_per_user_by_country_and_date_staging
),
subsession_hours_per_user_staging AS (
  SELECT
    submission_date_s3,
    country,
    SUM(subsession_hours_sum) AS `hours`,
    COUNT(DISTINCT client_id) AS users,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date_s3 = @submission_date
    AND app_name = 'Firefox'
    AND sample_id = 42
    AND subsession_hours_sum < 24
  GROUP BY
    submission_date_s3,
    country
),
subsession_hours_per_user AS (
  SELECT
    submission_date_s3,
    country,
    `hours` / users AS subsession_hours_per_user_ratio
  FROM
    subsession_hours_per_user_staging
),
active_hours_per_user_staging AS (
  SELECT
    submission_date_s3,
    country,
    SUM(active_hours_sum) AS `hours`,
    COUNT(DISTINCT(client_id)) AS users,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date_s3 = @submission_date
    AND app_name = 'Firefox'
    AND sample_id = 42
    AND active_hours_sum < 24
  GROUP BY
    submission_date_s3,
    country
),
active_hours_per_user AS (
  SELECT
    submission_date_s3,
    country,
    `hours` / users AS active_hours_per_user_ratio
  FROM
    active_hours_per_user_staging
),
default_percent_and_avg_age_by_country AS (
  SELECT
    submission_date_s3,
    country,
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
    AND app_name = 'Firefox'
    AND sample_id = 42
  GROUP BY
    submission_date_s3,
    country
)
SELECT
  COALESCE(
    COALESCE(spu.submission_date_s3, sshpu.submission_date_s3),
    ahpu.submission_date_s3
  ) AS submission_date,
  COALESCE(COALESCE(spu.country, sshpu.country), ahpu.country) AS country,
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
  searches_per_user_by_country_and_date AS spu
FULL OUTER JOIN
  subsession_hours_per_user AS sshpu
  ON spu.country = sshpu.country
FULL OUTER JOIN
  active_hours_per_user AS ahpu
  ON COALESCE(spu.country, sshpu.country) = ahpu.country
FULL OUTER JOIN
  default_percent_and_avg_age_by_country AS dflt
  ON COALESCE(COALESCE(spu.country, sshpu.country), ahpu.country) = dflt.country
