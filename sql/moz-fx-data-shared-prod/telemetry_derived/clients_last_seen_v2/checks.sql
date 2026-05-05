#warn
{{ is_unique(["client_id"], where="submission_date = @submission_date") }}

#warn
{{ min_row_count(1000, where="submission_date = @submission_date") }}

#fail
WITH daily AS (
  SELECT
    submission_date,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date
),
last_seen AS (
  SELECT
    submission_date,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND mozfun.bits28.days_since_seen(days_seen_bits) = 0
  GROUP BY
    submission_date
),
check_results AS (
  SELECT
    COUNTIF(last_seen.client_count IS DISTINCT FROM daily.client_count) AS client_count_diff
  FROM
    daily
  LEFT JOIN
    last_seen
    USING (submission_date)
)
SELECT
  IF(
    ABS((SELECT client_count_diff FROM check_results)) > 0,
    ERROR(
      CONCAT(
        "Count of clients doesn't match. clients_daily_v6 has ",
        STRING(((SELECT submission_date FROM daily))),
        ": ",
        ABS((SELECT client_count FROM daily)),
        ". Backfill has ",
        IFNULL(((SELECT client_count FROM daily)), 0)
      )
    ),
    NULL
  );
