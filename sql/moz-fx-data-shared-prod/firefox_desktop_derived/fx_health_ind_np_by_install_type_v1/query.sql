WITH first_install_type AS (
  SELECT
    client_id,
    REGEXP_REPLACE(`event`, 'installation.first_seen_', "") AS installer_type,
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_timestamp ASC) AS rnk
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    event_category = 'installation'
    AND sample_id = 0
    AND DATE(submission_timestamp) < @submission_date
  QUALIFY
    rnk = 1
),
staging AS (
  SELECT
    client_id,
    first_seen_date,
    submission_date,
    days_interacted_bits & days_visited_1_uri_bits AS active_days_bits
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen`
  WHERE
    first_seen_date = @fsd
    AND submission_date
    BETWEEN @fsd
    AND DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND sample_id = 0
),
cls AS (
  SELECT
    stg.client_id,
    stg.first_seen_date,
    fit.installer_type,
    stg.submission_date,
    stg.active_days_bits
  FROM
    staging stg
  JOIN
    first_install_type fit
    ON stg.client_id = fit.client_id
),
final_stg AS (
  SELECT
    first_seen_date,
    installer_type,
    COUNT(1) AS nbr_rows,
    COUNT(DISTINCT(CLIENT_ID)) AS new_profiles,
    SUM(BIT_COUNT(active_days_bits)) AS sum_active_days_bit_count_for_new_profiles
  FROM
    cls
  GROUP BY
    first_seen_date,
    installer_type
)
SELECT
  first_seen_date,
  installer_type,
  nbr_rows,
  new_profiles,
  sum_active_days_bit_count_for_new_profiles,
  SAFE_DIVIDE(
    sum_active_days_bit_count_for_new_profiles,
    new_profiles
  ) AS ratio_of_np_days_active_bits_vs_np_first_7_days
FROM
  final_stg
