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
)
SELECT
  COALESCE(
    COALESCE(spu.submission_date_s3, sshpu.submission_date_s3),
    ahpu.submission_date_s3
  ) AS submission_date,
  COALESCE(COALESCE(spu.country, sshpu.country), ahpu.country) AS country,
  spu.searches_per_user_ratio,
  sshpu.subsession_hours_per_user_ratio,
  ahpu.active_hours_per_user_ratio
FROM
  searches_per_user_by_country_and_date AS spu
FULL OUTER JOIN
  subsession_hours_per_user AS sshpu
  ON spu.country = sshpu.country
FULL OUTER JOIN
  active_hours_per_user AS ahpu
  ON COALESCE(spu.country, sshpu.country) = ahpu.country
