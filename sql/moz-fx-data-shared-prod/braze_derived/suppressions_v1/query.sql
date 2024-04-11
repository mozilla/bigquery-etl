WITH
  suppressions AS (
  SELECT
    LOWER(email) AS email,
    external_id AS email_id
  FROM
    `moz-fx-data-shared-prod.acoustic_external.suppression_list_v1`
  UNION DISTINCT
  SELECT
    LOWER(primary_email) AS email,
    email_id
  FROM
    `moz-fx-data-shared-prod.ctms_braze.ctms_emails`
  WHERE
    has_opted_out_of_email = TRUE
  UNION DISTINCT
  SELECT
    LOWER(email) AS email,
    email_id
  FROM
    `moz-fx-data-shared-prod.acoustic_external.contact_raw_v1`
  WHERE
    has_opted_out_of_email = 1)
SELECT
  *,
  @submission_date AS last_modified_timestamp
FROM
  suppressions
  