
#warn
WITH daily_users_sum AS (
  SELECT
    SUM(daily_users),
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
),
distinct_client_count_base AS (
  SELECT
    COUNT(DISTINCT client_info.client_id) AS distinct_client_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_klar_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
distinct_client_count AS (
  SELECT
    SUM(distinct_client_count)
  FROM
    distinct_client_count_base
)
SELECT
  IF(
    ABS((SELECT * FROM daily_users_sum) - (SELECT * FROM distinct_client_count)) > 10,
    ERROR(
      CONCAT(
        "Daily users mismatch between the klar_ios live across all channels (`moz-fx-data-shared-prod.org_mozilla_ios_klar_live.baseline_v1`,) and active_users_aggregates (`{{ dataset_id }}.{{ table_name }}`) tables is greater than 10.",
        " Live table count: ",
        (SELECT * FROM distinct_client_count),
        " | active_users_aggregates (daily_users): ",
        (SELECT * FROM daily_users_sum),
        " | Delta detected: ",
        ABS((SELECT * FROM daily_users_sum) - (SELECT * FROM distinct_client_count))
      )
    ),
    NULL
  );

#fail
WITH dau_current AS (
  SELECT
    SUM(dau) AS dau
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
),
dau_previous AS (
  SELECT
    SUM(dau) AS dau
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
)
SELECT
  IF(
    ABS((SELECT SUM(dau) FROM dau_current) / (SELECT SUM(dau) FROM dau_previous)) > 1.5,
    ERROR(
      "Current date's DAU is 50% higher than in previous date. See source table (`{{ project_id }}.{{ dataset_id }}.{{ table_name }}`)!"
    ),
    NULL
  );
