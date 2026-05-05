WITH first_install_type AS (
  SELECT
    client_id,
    event_object AS installer_type,
  FROM
    `moz-fx-data-shared-prod.telemetry.events`
  WHERE
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 14 DAY)
    AND @submission_date
    AND event_category = 'installation'
    AND sample_id = 0
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_date ASC) = 1
),
new_profile_activity AS (
  SELECT
    first_seen_date,
    client_id,
    days_interacted_bits & days_visited_1_uri_bits AS active_days_bits
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_last_seen`
  WHERE
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND first_seen_date = DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND sample_id = 0
),
new_profile_activity_with_installer AS (
  SELECT
    client_id,
    first_seen_date,
    installer_type,
    active_days_bits,
  FROM
    new_profile_activity
  INNER JOIN
    first_install_type
    USING (client_id)
),
final AS (
  SELECT
    first_seen_date,
    installer_type,
    COUNT(*) AS nbr_rows,
    COUNT(DISTINCT(client_id)) AS new_profiles,
    SUM(BIT_COUNT(active_days_bits)) AS sum_active_days_bit_count_for_new_profiles
  FROM
    new_profile_activity_with_installer
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
  final
