WITH ctms_emails AS (
  SELECT
    emails.email_id AS external_id,
    LOWER(emails.primary_email) AS email,
    NULLIF(LOWER(emails.mailing_country), '') AS mailing_country,
    CASE
      WHEN emails.double_opt_in = TRUE
        THEN 'opted_in'
      WHEN emails.double_opt_in = FALSE
        AND emails.has_opted_out_of_email = FALSE
        THEN 'subscribed'
      ELSE 'unsubscribed'
    END AS email_subscribe,
    emails.basket_token,
    NULLIF(LOWER(emails.email_lang), '') AS email_lang,
    TO_HEX(SHA256(fxa.fxa_id)) AS fxa_id_sha256,
    CASE
      WHEN fxa.fxa_id IS NOT NULL
        AND fxa.account_deleted = FALSE
        THEN TRUE
    END AS has_fxa,
    LOWER(fxa.primary_email) AS fxa_primary_email,
    NULLIF(LOWER(fxa.lang), '') AS fxa_lang,
    NULLIF(LOWER(fxa.first_service), '') AS fxa_first_service,
    CAST(fxa.created_date AS TIMESTAMP) AS fxa_created_at,
    emails.create_timestamp,
    emails.update_timestamp
  FROM
    `moz-fx-data-shared-prod.ctms_braze.ctms_emails` AS emails
  LEFT JOIN
    `moz-fx-data-shared-prod.ctms_braze.ctms_fxa` AS fxa
    ON emails.email_id = fxa.email_id
),
active_users AS (
  SELECT
    emails.external_id,
    emails.email,
    emails.email_subscribe,
    emails.mailing_country,
    emails.basket_token,
    emails.email_lang,
    emails.fxa_id_sha256,
    emails.has_fxa,
    emails.fxa_primary_email,
    emails.fxa_lang,
    emails.fxa_first_service,
    emails.fxa_created_at,
    emails.create_timestamp,
    emails.update_timestamp
  FROM
    ctms_emails AS emails
  LEFT JOIN
    `moz-fx-data-shared-prod.marketing_suppression_list_derived` AS suppressions
    ON emails.email = suppressions.email
  LEFT JOIN
    `moz-fx-data-shared-prod.ctms_braze.ctms_fxa` AS fxa
    ON emails.external_id = fxa.email_id
  WHERE
    suppressions.email IS NULL -- exclude users on suppression list
    AND has_opted_out_of_email =  false -- has not opted out of all newsletters
    -- ensure user is associated w/ active subscription or product
    AND (
      EXISTS(
        SELECT
          1
        FROM
          `moz-fx-data-shared-prod.ctms_braze.ctms_newsletters` AS newsletters
        WHERE
          newsletters.email_id = emails.external_id
          AND newsletters.subscribed = TRUE
      )
      OR EXISTS(
        SELECT
          1
        FROM
          `moz-fx-data-shared-prod.ctms_braze.ctms_waitlists` AS waitlists
        WHERE
          waitlists.email_id = emails.external_id
          AND waitlists.subscribed = TRUE
      )
      OR EXISTS(
        SELECT
          1
        FROM
          `moz-fx-data-shared-prod.subscription_platform.logical_subscriptions` AS products
        WHERE
          products.mozilla_account_id_sha256 = emails.fxa_id_sha256
      )
    )
)
SELECT
  external_id,
  email,
  email_subscribe,
  mailing_country,
  basket_token,
  email_lang,
  fxa_id_sha256,
  has_fxa,
  fxa_primary_email,
  fxa_lang,
  fxa_first_service,
  fxa_created_at,
  create_timestamp,
  update_timestamp
FROM
  active_users;
