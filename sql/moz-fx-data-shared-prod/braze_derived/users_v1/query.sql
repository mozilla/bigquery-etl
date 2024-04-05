CREATE OR REPLACE TABLE
  braze_external.dev_users AS
WITH
  exclusions AS (
  SELECT
    e.email_id,
    e.primary_email AS email,
    e.mailing_country,
    CASE
      WHEN e.double_opt_in = TRUE THEN 'opted_in'
      WHEN e.double_opt_in = FALSE
    AND e.has_opted_out_of_email = FALSE THEN 'subscribed'
    ELSE
    'unsubscribed'
  END
    AS email_subscribe,
    e.basket_token,
    e.email_lang,
    COALESCE( TO_HEX(SHA256(f.fxa_id)), TO_HEX(SHA256(fxa.fxa_id)) ) AS fxa_id_sha256,
    f.primary_email AS fxa_primary_email,
    f.lang AS fxa_lang,
    f.first_service,
    'emails' AS SOURCE,
    DATE(e.update_timestamp) AS last_modified_date
  FROM
    `moz-fx-data-shared-prod.ctms_braze.ctms_emails` e
  LEFT JOIN
    `moz-fx-data-shared-prod.ctms_braze.ctms_fxa` f
  ON
    e.email_id = f.email_id
  LEFT JOIN
    `moz-fx-data-shared-prod.ctms.ctms_fxa` fxa
  ON
    e.email_id = fxa.email_id
  LEFT JOIN
    `moz-fx-data-shared-prod.braze_external.dev_suppressions` s
  ON
    LOWER(e.primary_email) = s.email
  WHERE
    s.email IS NULL
  UNION ALL
  SELECT
    c.email_id,
    c.email,
    c.mailing_country,
    CASE
      WHEN c.double_opt_in = 1 THEN 'opted_in'
      WHEN c.double_opt_in = 0
    AND c.has_opted_out_of_email = 0 THEN 'subscribed'
    ELSE
    'unsubscribed'
  END
    AS email_subscribe,
    c.basket_token,
    c.email_lang,
    TO_HEX(SHA256(c.fxa_id)) AS fxa_id_sha256,
    CAST(NULL AS STRING) AS fxa_primary_email,
    CAST(NULL AS STRING) AS fxa_lang,
    CAST(NULL AS STRING) AS first_service,
    'contacts' AS SOURCE,
    c.last_modified_date
  FROM
    `moz-fx-data-shared-prod.acoustic_external.contact_raw_v1` c
  LEFT JOIN
    `moz-fx-data-shared-prod.braze_external.dev_suppressions` s
  ON
    LOWER(c.email) = s.email
  WHERE
    s.email IS NULL ),
  ranked_exclusions AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY email_id ORDER BY last_modified_date DESC) AS rn
  FROM
    exclusions )
SELECT
  email_id,
  email,
  mailing_country,
  email_subscribe,
  basket_token,
  email_lang,
  fxa_id_sha256,
  fxa_primary_email,
  fxa_lang,
  first_service,
  SOURCE
FROM
  ranked_exclusions
WHERE
  rn = 1;
