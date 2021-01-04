SELECT
  timestamp,
  jsonPayload.fields.event,
  jsonPayload.fields.flow_id,
  jsonPayload.fields.entrypoint,
  jsonPayload.fields.service,
  jsonPayload.fields.useragent,
  jsonPayload.fields.os_version,
  jsonPayload.fields.utm_source,
  jsonPayload.fields.utm_campaign,
  jsonPayload.fields.utm_content,
  jsonPayload.fields.utm_medium,
  jsonPayload.fields.utm_term
FROM
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_auth_20*`
WHERE
  -- We exclude event types that are Amplitude events;
  -- these are already captured in the fxa_auth_events_v1 table.
  NOT (
    REGEXP_CONTAINS(jsonPayload.fields.event, r"^email\.(\w+)\.bounced$")
    OR REGEXP_CONTAINS(jsonPayload.fields.event, r"^email\.(\w+)\.sent$")
    OR REGEXP_CONTAINS(jsonPayload.fields.event, r"^flow\.complete\.(\w+)$")
    OR jsonPayload.fields.event IN (
      'account.confirmed',
      'account.login',
      'account.login.blocked',
      'account.login.confirmedUnblockCode',
      'account.reset',
      'account.signed',
      'account.created',
      'account.verified',
      'sms.installFirefox.sent'
    )
  )
  AND _TABLE_SUFFIX = FORMAT_DATE('%y%m%d', @submission_date)
