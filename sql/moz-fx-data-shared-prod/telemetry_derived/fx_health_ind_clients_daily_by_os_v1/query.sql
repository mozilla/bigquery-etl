WITH searches_per_user_by_os_and_date_staging
AS
  (
    SELECT
      submission_date_s3,
      os,
      SUM(search_count_all) AS searches,
      COUNT(DISTINCT client_id) AS users,
    FROM
      `moz-fx-data-shared-prod.telemetry.clients_daily`
    WHERE
      submission_date_s3 = @submission_date
      AND app_name = 'Firefox'
      AND sample_id = 42
      AND search_count_all < 10000
      AND os IN ('Windows_NT', 'Darwin', 'Linux')
    GROUP BY
      submission_date_s3,
      os
  ),
  searches_per_user_by_os_and_date AS (
    SELECT
      submission_date_s3,
      os,
      searches / users AS searches_per_user_ratio,
    FROM
      searches_per_user_by_os_and_date_staging
  ),
  subsession_hours_per_user_staging AS (
    SELECT
      submission_date_s3,
      os,
      SUM(subsession_hours_sum) AS `hours`,
      count(DISTINCT client_id) AS users,
    FROM
      `moz-fx-data-shared-prod.telemetry.clients_daily`
    WHERE
      submission_date_s3 = @submission_date
      AND app_name = 'Firefox'
      AND sample_id = 42
      AND subsession_hours_sum < 24
      AND os IN ('Windows_NT', 'Darwin', 'Linux')
    GROUP BY
      submission_date_s3,
      os
  ),
  subsession_hours_per_user AS (
    SELECT
      submission_date_s3,
      os,
      `hours` / users AS subsession_hours_per_user_ratio
    FROM
      subsession_hours_per_user_staging
  ),
  active_hours_per_user_staging AS (
    SELECT
      submission_date_s3,
      os,
      SUM(ROUND(active_hours_sum)) AS `hours`,
      COUNT(DISTINCT(client_id)) AS users,
    FROM
      `moz-fx-data-shared-prod.telemetry.clients_daily`
    WHERE
      submission_date_s3 = @submission_date
      AND app_name = 'Firefox'
      AND sample_id = 42
      AND os IN ('Windows_NT', 'Darwin', 'Linux')
    GROUP BY
      submission_date_s3,
      os
  ),
  active_hours_per_user AS (
    SELECT
      submission_date_s3,
      os,
      `hours` / users AS active_hours_per_user_ratio
    FROM
      active_hours_per_user_staging
  )
SELECT
  COALESCE(
    COALESCE(spu.submission_date_s3, sshpu.submission_date_s3),
    ahpu.submission_date_s3
  ) AS submission_date,
  COALESCE(COALESCE(spu.os, sshpu.os), ahpu.os) AS os,
  spu.searches_per_user_ratio,
  sshpu.subsession_hours_per_user_ratio,
  ahpu.active_hours_per_user_ratio
FROM
  searches_per_user_by_os_and_date AS spu
FULL OUTER JOIN
  subsession_hours_per_user AS sshpu
  ON spu.os = sshpu.os
FULL OUTER JOIN
  active_hours_per_user AS ahpu
  ON COALESCE(spu.os, sshpu.os) = ahpu.os
