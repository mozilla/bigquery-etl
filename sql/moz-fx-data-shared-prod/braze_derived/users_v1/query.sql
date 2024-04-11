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
    e.create_timestamp AS create_timestamp,
    e.update_timestamp AS update_timestamp,
    @submission_date AS last_modified_timestamp,
    DATE(@submission_date) AS last_modified_date
  FROM
    `moz-fx-data-shared-prod.ctms_braze.ctms_emails` e
  LEFT JOIN
    `moz-fx-data-shared-prod.braze_derived.suppressions_v1` s
  ON
    LOWER(e.primary_email) = s.email
  WHERE
    s.email IS NULL )
SELECT
  email_id,
  email,
  mailing_country,
  email_subscribe,
  basket_token,
  email_lang,
  create_timestamp,
  update_timestamp,
  @submission_date AS last_modified_timestamp,
  DATE(@submission_date) AS last_modified_date
FROM
  exclusions;
