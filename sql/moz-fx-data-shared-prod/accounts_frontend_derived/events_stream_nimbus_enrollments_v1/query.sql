WITH cirrus_data AS (
  SELECT DISTINCT
    mozfun.map.get_key(e.extra, "experiment") AS experiment_id,
    mozfun.map.get_key(e.extra, "branch") AS branch,
    mozfun.map.get_key(e.extra, "nimbus_user_id") AS user_id,
  FROM
    `moz-fx-data-shared-prod.accounts_cirrus.enrollment` AS enrollment
  CROSS JOIN
    UNNEST(events) AS e
  WHERE
    DATE(submission_timestamp) = @submission_date
)
SELECT
  acct_fr.event_id,
  acct_fr.additional_properties,
  acct_fr.client_info,
  acct_fr.document_id,
  acct_fr.metadata,
  acct_fr.normalized_app_name,
  acct_fr.normalized_channel,
  acct_fr.normalized_country_code,
  acct_fr.normalized_os,
  acct_fr.normalized_os_version,
  acct_fr.ping_info,
  acct_fr.sample_id,
  acct_fr.submission_timestamp,
  acct_fr.client_id,
  acct_fr.reason,
  acct_fr.experiments,
  acct_fr.event_timestamp,
  acct_fr.event_category,
  acct_fr.event_name,
  acct_fr.event,
  acct_fr.event_extra,
  acct_fr.metrics,
  acct_fr.profile_group_id,
  acct_fr.legacy_telemetry_client_id,
  acct_fr.app_version_major,
  acct_fr.app_version_minor,
  acct_fr.app_version_patch,
  acct_fr.is_bot_generated,
  acct_fr.document_event_number,
  nimbus.experiment_id,
  nimbus.branch,
  @submission_date AS submission_date
FROM
  `moz-fx-data-shared-prod.accounts_frontend.events_stream` AS acct_fr
LEFT JOIN
  cirrus_data AS nimbus
  ON nimbus.user_id = acct_fr.metrics.string.account_user_id
WHERE
  DATE(submission_timestamp) = @submission_date
