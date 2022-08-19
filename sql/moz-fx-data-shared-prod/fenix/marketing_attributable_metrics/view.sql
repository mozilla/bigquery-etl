CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.marketing_attributable_metrics`
AS
WITH
  dau_client AS (
  SELECT
    submission_date,
    client_id,
    first_seen_date,
    country,
    COUNTIF(days_since_seen < 1) AS dau,
    COUNTIF(first_seen_date = submission_date) AS new_profiles
  FROM
    `moz-fx-data-shared-prod.telemetry.fenix_clients_last_seen`
  WHERE
    submission_date >= '2021-01-01'
    AND days_since_seen < 1
  GROUP BY 1, 2, 3, 4
),
  adjust_client AS (
  SELECT
    client_info.client_id AS client_id,
    ARRAY_AGG(metrics.string.first_session_network)[SAFE_OFFSET(0)] AS adjust_network,
    ARRAY_AGG(metrics.string.first_session_adgroup)[SAFE_OFFSET(0)] AS adjust_adgroup,
    ARRAY_AGG(metrics.string.first_session_campaign)[SAFE_OFFSET(0)] AS adjust_campaign,
    ARRAY_AGG(metrics.string.first_session_creative)[SAFE_OFFSET(0)] AS adjust_creative,
    MIN(DATE(submission_timestamp)) as first_session_date
  FROM `mozdata.fenix.first_session`
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND metrics.string.first_session_network IS NOT NULL
    AND metrics.string.first_session_network <> ''
  GROUP BY 1
)
SELECT client_id, country, first_seen_date as cohort_date, first_session_date, submission_date, dau, new_profiles, adjust_network, adjust_adgroup, adjust_campaign, adjust_creative
FROM dau_client
JOIN adjust_client USING (client_id)
