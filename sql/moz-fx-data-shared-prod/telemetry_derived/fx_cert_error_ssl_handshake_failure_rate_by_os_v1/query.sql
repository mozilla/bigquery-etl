WITH base AS (
  SELECT
    SUM(ssl_handshake_result_failure_sum) AS failures,
    SUM(ssl_handshake_result_success_sum) AS success,
    CAST(
      SAFE_DIVIDE(
        SUM(ssl_handshake_result_failure_sum),
        SUM(ssl_handshake_result_success_sum)
      ) AS FLOAT64
    ) AS ratio,
    os,
    submission_date
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date_s3 = @submission_date
    AND app_name = 'Firefox'
  GROUP BY
    submission_date,
    os
)
SELECT
  *
FROM
  base
