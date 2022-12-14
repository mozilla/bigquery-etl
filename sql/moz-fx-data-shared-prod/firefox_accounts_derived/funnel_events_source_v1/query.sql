SELECT
  DATE(timestamp) AS submission_date,
  user_id AS client_id,
  `moz-fx-data-shared-prod`.udf.safe_sample_id(user_id) AS sample_id,
  SPLIT(event_type, ' - ')[OFFSET(0)] AS category,
  SPLIT(event_type, ' - ')[OFFSET(1)] AS event,
  [
    STRUCT('service' AS key, service AS value),
    STRUCT('email_type' AS key, email_type AS value),
    STRUCT('oauth_client_id' AS key, oauth_client_id AS value),
    STRUCT('connect_device_flow' AS key, connect_device_flow AS value),
    STRUCT('connect_device_os' AS key, connect_device_os AS value),
    STRUCT('sync_device_count' AS key, sync_device_count AS value),
    STRUCT('email_sender' AS key, email_sender AS value),
    STRUCT('email_service' AS key, email_service AS value),
    STRUCT('email_template' AS key, email_template AS value),
    STRUCT('email_version' AS key, email_version AS value)
  ] AS extra,
  CAST([] AS ARRAY<STRUCT<key STRING, value STRING>>) AS experiments,
  *,
FROM
  `moz-fx-data-shared-prod.firefox_accounts.fxa_content_auth_oauth_events`
WHERE
  DATE(timestamp) = @submission_date
