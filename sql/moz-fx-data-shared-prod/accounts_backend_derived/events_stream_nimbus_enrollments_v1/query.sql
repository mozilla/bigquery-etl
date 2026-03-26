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
  acct_bck.event_id,
  acct_bck.additional_properties,
  acct_bck.client_info,
  acct_bck.document_id,
  acct_bck.metadata,
  acct_bck.metrics,
  acct_bck.normalized_app_name,
  acct_bck.normalized_channel,
  acct_bck.normalized_country_code,
  acct_bck.normalized_os,
  acct_bck.normalized_os_version,
  acct_bck.ping_info,
  acct_bck.sample_id,
  acct_bck.submission_timestamp,
  acct_bck.client_id,
  acct_bck.reason,
  acct_bck.experiments,
  acct_bck.event_timestamp,
  acct_bck.event_category,
  acct_bck.event_name,
  acct_bck.event,
  acct_bck.event_extra,
  acct_bck.profile_group_id,
  acct_bck.legacy_telemetry_client_id,
  acct_bck.app_version_major,
  acct_bck.app_version_minor,
  acct_bck.app_version_patch,
  acct_bck.is_bot_generated,
  acct_bck.document_event_number,
  acct_bck.extras,
  nimbus.experiment_id,
  nimbus.branch
FROM
  `moz-fx-data-shared-prod.accounts_backend.events_stream` AS acct_bck
LEFT JOIN
  cirrus_data AS nimbus
  ON nimbus.user_id = acct_bck.metrics.string.account_user_id
WHERE
  DATE(submission_timestamp) = @submission_date
