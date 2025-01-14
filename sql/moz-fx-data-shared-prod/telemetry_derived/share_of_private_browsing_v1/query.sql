SELECT
  submission_date_s3 AS submission_date,
  SUM(total_uri_count_private_mode) AS total_uri_count_private_mode,
  SUM(total_uri_count_normal_mode) AS total_uri_count_normal_mode,
  --for users who used private mode that day, what was their total URI count in normal mode
  --i.e. excluding users who never used private mode that day
  SUM(
    CASE
      WHEN total_uri_count_private_mode > 0
        THEN total_uri_count_normal_mode
      ELSE 0
    END
  ) AS total_uri_count_normal_mode_for_users_using_pbm_on_date
FROM
  `moz-fx-data-shared-prod.telemetry.clients_daily`
WHERE
  submission_date_s3 = @submission_date
  AND app_name = 'Firefox'
GROUP BY
  submission_date_s3
