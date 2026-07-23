-- Query for firefox_accounts_derived.fxa_multi_device_account_shape_daily_v1
-- Week-to-date counts are re-derived from raw pings each run (Monday through
-- @submission_date) rather than carried forward from the prior day's row,
-- since the source ping tables only retain 30 days of history.
WITH all_clients AS (
  SELECT
    client_info.client_id AS client_id,
    metrics.string.client_association_uid AS account_uid,
    'desktop' AS device_type
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.fx_accounts`
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_TRUNC(@submission_date, WEEK(MONDAY))
    AND @submission_date
    AND metrics.string.client_association_uid IS NOT NULL
    AND metrics.string.client_association_uid != ''
    AND client_info.client_id IS NOT NULL
  UNION ALL
  SELECT
    client_info.client_id AS client_id,
    metrics.string.user_client_association_uid AS account_uid,
    'mobile' AS device_type
  FROM
    `moz-fx-data-shared-prod.firefox_ios.fx_accounts`
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_TRUNC(@submission_date, WEEK(MONDAY))
    AND @submission_date
    AND metrics.string.user_client_association_uid IS NOT NULL
    AND metrics.string.user_client_association_uid != ''
    AND client_info.client_id IS NOT NULL
  UNION ALL
  SELECT
    client_info.client_id AS client_id,
    metrics.string.client_association_uid AS account_uid,
    'mobile' AS device_type
  FROM
    `moz-fx-data-shared-prod.fenix.fx_accounts`
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_TRUNC(@submission_date, WEEK(MONDAY))
    AND @submission_date
    AND metrics.string.client_association_uid IS NOT NULL
    AND metrics.string.client_association_uid != ''
    AND client_info.client_id IS NOT NULL
),
account_week_shape AS (
  SELECT
    account_uid,
    COUNT(DISTINCT IF(device_type = 'desktop', client_id, NULL)) AS desktop_clients,
    COUNT(DISTINCT IF(device_type = 'mobile', client_id, NULL)) AS mobile_clients
  FROM
    all_clients
  GROUP BY
    account_uid
)
SELECT
  @submission_date AS submission_date,
  DATE_TRUNC(@submission_date, WEEK(MONDAY)) AS week_start,
  desktop_clients,
  mobile_clients,
  COUNT(DISTINCT account_uid) AS active_accounts
FROM
  account_week_shape
GROUP BY
  week_start,
  desktop_clients,
  mobile_clients
