SELECT
  recipient_id,
  report_id,
  mailing_id,
  campaign_id,
  content_id,
  PARSE_DATETIME("%m/%d/%Y %H:%M:%S", event_timestamp) AS event_timestamp,
  LOWER(COALESCE(NULLIF(event_type, ''), "unknown")) AS event_type,
  LOWER(COALESCE(NULLIF(recipient_type, ''), "unknown")) AS recipient_type,
  LOWER(COALESCE(NULLIF(body_type, ''), "unknown")) AS body_type,
  LOWER(COALESCE(NULLIF(click_name, ''), "unknown")) AS click_name,
  COALESCE(NULLIF(url, ''), "unknown") AS url,
  LOWER(COALESCE(NULLIF(suppression_reason, ''), "unknown")) AS suppression_reason,
  submission_date,
FROM
  `moz-fx-data-marketing-prod.acoustic.raw_recipient_export_raw_v1`
WHERE
  submission_date = DATE(@submission_date);
