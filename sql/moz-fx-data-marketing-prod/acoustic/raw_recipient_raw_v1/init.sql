CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.acoustic.raw_recipient_raw_v1`(
    email STRING,
    recipient_id INTEGER,
    report_id INTEGER,
    mailing_id INTEGER,
    campaign_id INTEGER,
    content_id STRING,
    event_timestamp STRING,
    event_type STRING,
    recipient_type STRING,
    body_type STRING,
    click_name STRING,
    url STRING,
    suppression_reason STRING,
    submission_date DATE
  )
PARTITION BY
  submission_date
CLUSTER BY
  event_type,
  recipient_type,
  body_type
