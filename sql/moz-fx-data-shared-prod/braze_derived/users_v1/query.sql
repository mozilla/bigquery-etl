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
    has_opted_out_of_email,
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
    `moz-fx-data-shared-prod.marketing_suppression_list_derived.main_suppression_list_v1` AS suppressions
    ON emails.email = suppressions.email
  LEFT JOIN
    `moz-fx-data-shared-prod.ctms_braze.ctms_fxa` AS fxa
    ON emails.external_id = fxa.email_id
  WHERE
    suppressions.email IS NULL -- exclude users on suppression list
    AND emails.has_opted_out_of_email =  false -- has not opted out of all newsletters
    -- ensure user is associated w/ active subscription or product
    AND fxa.account_deleted = false -- has not deleted FxA
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
),
filtered_out_mofo_users AS (
  SELECT
    active_users.external_id,
    active_users.email,
    active_users.email_subscribe,
    active_users.mailing_country,
    active_users.basket_token,
    active_users.email_lang,
    active_users.fxa_id_sha256,
    active_users.has_fxa,
    active_users.fxa_primary_email,
    active_users.fxa_lang,
    active_users.fxa_first_service,
    active_users.fxa_created_at,
    active_users.create_timestamp,
    active_users.update_timestamp
  FROM
    active_users AS active_users
  WHERE
    -- Check if user is not subscribed only to 'mozilla-foundation' and has at least one other subscription
    (
      NOT EXISTS (
        SELECT 1
        FROM `moz-fx-data-shared-prod.ctms_braze.ctms_newsletters` AS mofo_newsletters
        WHERE mofo_newsletters.email_id = active_users.external_id
        AND mofo_newsletters.subscribed = TRUE
        AND mofo_newsletters.name = 'mozilla-foundation'
      )
      OR EXISTS (
        SELECT 1
        FROM `moz-fx-data-shared-prod.ctms_braze.ctms_newsletters` AS other_nl
        WHERE other_nl.email_id = active_users.external_id
        AND other_nl.subscribed = TRUE
        AND other_nl.name != 'mozilla-foundation'
      )
      OR EXISTS (
        SELECT 1
        FROM `moz-fx-data-shared-prod.ctms_braze.ctms_waitlists` AS waitlists
        WHERE waitlists.email_id = active_users.external_id
        AND waitlists.subscribed = TRUE
      )
      OR EXISTS (
        SELECT 1
        FROM `moz-fx-data-shared-prod.subscription_platform.logical_subscriptions` AS subscriptions
        WHERE subscriptions.mozilla_account_id_sha256 = active_users.fxa_id_sha256
      )
    )
)

SELECT COUNT(*) FROM filtered_out_mofo_users;
