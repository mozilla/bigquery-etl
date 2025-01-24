--Get clients who had the first seen date 8 days prior to submission date
WITH cfs AS (
  SELECT
    client_id,
    first_seen_date
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_first_seen`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 8 DAY)
),
--Get clients who saw onboarding on or before their first seen date (i.e. 8 days prior to submission date)
onboarding AS (
  SELECT DISTINCT
    client_id,
    TRUE AS saw_onboarding
  FROM
    `moz-fx-data-shared-prod.messaging_system.onboarding`
  WHERE
    DATE(submission_timestamp) <= DATE_SUB(@submission_date, INTERVAL 8 DAY)
    AND event_context LIKE '%about:welcome%'
    AND event = 'IMPRESSION'
),
--Get a flag for if the client used private browsing mode on their first seen date
--i.e. 8 days prior to submision date
pbm_usage_on_first_seen_date AS (
  SELECT
    client_id,
    dom_parentprocess_private_window_used AS used_pbm_on_first_seen_date
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_last_seen`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 8 DAY)
),
is_dau_in_next_7_days AS (
  SELECT
    client_id,
    MAX(is_dau) AS active_in_next_7_days
  FROM
    `moz-fx-data-shared-prod.telemetry.desktop_active_users`
  WHERE
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND DATE_SUB(@submission_date, INTERVAL 1 DAY)
  GROUP BY
    client_id
)
SELECT
  @submission_date AS submission_date,
  c.first_seen_date,
  COALESCE(o.saw_onboarding, FALSE) AS saw_onboarding,
  p.used_pbm_on_first_seen_date,
  r.active_in_next_7_days,
  COUNT(DISTINCT(c.client_id)) AS nbr_clients
FROM
  cfs AS c
JOIN
  pbm_usage_on_first_seen_date AS p
  ON c.client_id = p.client_id
LEFT JOIN
  onboarding AS o
  ON c.client_id = o.client_id
LEFT JOIN
  is_dau_in_next_7_days AS r
  ON c.client_id = r.client_id
GROUP BY
  @submission_date,
  c.first_seen_date,
  o.saw_onboarding,
  p.used_pbm_on_first_seen_date,
  r.active_in_next_7_days
