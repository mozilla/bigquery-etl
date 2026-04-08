-- Generated via bigquery_etl.glean_usage

#fail
WITH daily AS (
  SELECT
    submission_date,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `{{ project_id }}.{{ dataset_id }}.baseline_clients_daily_v1`
  WHERE
    submission_date = @submission_date
    AND sample_id IS NOT NULL
  GROUP BY
    submission_date
),
last_seen AS (
  SELECT
    submission_date,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `{{ project_id }}.{{ dataset_id }}.baseline_clients_last_seen_v1`
  WHERE
    submission_date = @submission_date
    AND mozfun.bits28.days_since_seen(days_seen_bits) = 0
  GROUP BY
    submission_date
),
check_results AS (
  SELECT
    1 - (last_seen.client_count / daily.client_count) AS difference_perc
  FROM
    daily
  LEFT JOIN
    last_seen
    USING (submission_date)
)
SELECT
  IF(
    ABS((SELECT difference_perc FROM check_results)) > 0.001,
    ERROR(
      CONCAT(
        "Results don't match by > 1%, baseline_clients_daily table has ",
        STRING(((SELECT submission_date FROM daily))),
        ": ",
        ABS((SELECT client_count FROM daily)),
        ". baseline_clients_last_seen has ",
        IFNULL(((SELECT client_count FROM last_seen)), 0)
      )
    ),
    NULL
  );
