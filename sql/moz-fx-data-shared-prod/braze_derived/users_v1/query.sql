WITH ctms_emails AS (
  SELECT
    emails.email_id AS external_id,
    LOWER(emails.primary_email) AS email,
    LOWER(emails.mailing_country) AS mailing_country,
    CASE
      WHEN emails.double_opt_in = TRUE
        THEN 'opted_in'
      WHEN emails.double_opt_in = FALSE
        AND emails.has_opted_out_of_email = FALSE
        THEN 'subscribed'
      ELSE 'unsubscribed'
    END AS email_subscribe,
    emails.basket_token,
    LOWER(emails.email_lang) AS email_lang,
    TO_HEX(SHA256(fxa.fxa_id)) AS fxa_id_sha256,
    CASE
      WHEN fxa.fxa_id IS NOT NULL
        AND fxa.account_deleted = FALSE
        THEN TRUE
    END AS has_fxa,
    LOWER(fxa.primary_email) AS fxa_primary_email,
    LOWER(fxa.lang) AS fxa_lang,
    LOWER(fxa.first_service) AS first_service,
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
    emails.first_service,
    emails.create_timestamp,
    emails.update_timestamp
  FROM
    ctms_emails AS emails
  LEFT JOIN
    `moz-fx-data-shared-prod.braze_derived.suppressions_v1` AS suppressions
    ON emails.email = suppressions.email
  LEFT JOIN
    `moz-fx-data-shared-prod.ctms_braze.ctms_fxa` AS fxa
    ON emails.external_id = fxa.email_id
  WHERE
    -- exclude users on suppression list
    suppressions.email IS NULL
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
          `moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_v1` AS products
        WHERE
          products.fxa_uid = emails.fxa_id_sha256
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
  first_service,
  create_timestamp,
  update_timestamp
FROM
  active_users;
