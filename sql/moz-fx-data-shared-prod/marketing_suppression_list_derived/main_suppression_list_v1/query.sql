WITH suppressions AS (
  -- CTMS `has_opted_out_of_email` emails
  SELECT
    LOWER(primary_email) AS email,
  FROM
    `moz-fx-data-shared-prod.ctms_braze.ctms_emails`
  WHERE
    has_opted_out_of_email = TRUE
  -- Acoustic Suppression List
  UNION DISTINCT
  SELECT
    LOWER(email) AS email
  FROM
    `moz-fx-data-shared-prod.acoustic_external.suppression_list_v1`
  -- braze unsubscribes
  UNION DISTINCT
  SELECT
    email_address AS email
  FROM
    `moz-fx-data-shared-prod.braze_external.unsubscribes_v1`
  -- braze hard bounces
  UNION DISTINCT
  SELECT
    email_address AS email
  FROM
    `moz-fx-data-shared-prod.braze_external.hard_bounces_v1`
-- MoFo suppression List
  UNION DISTINCT
  SELECT
    email
  FROM
    `moz-fx-data-shared-prod.marketing_suppression_list_external.campaign_monitor_suppression_list_v1`
)
SELECT
  *
FROM
  suppressions
