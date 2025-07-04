WITH ctms_emails AS (
  SELECT
    emails.email_id AS external_id,
    LOWER(emails.primary_email) AS email,
    NULLIF(LOWER(emails.mailing_country), '') AS mailing_country,
    CASE
      WHEN emails.has_opted_out_of_email = TRUE
        THEN "unsubscribed"
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
      ELSE FALSE
    END AS has_fxa,
    LOWER(fxa.primary_email) AS fxa_primary_email,
    NULLIF(LOWER(fxa.lang), '') AS fxa_lang,
    NULLIF(LOWER(fxa.first_service), '') AS fxa_first_service,
    has_opted_out_of_email,
    CAST(fxa.created_date AS TIMESTAMP) AS fxa_created_at,
    emails.create_timestamp AS create_timestamp,
    emails.update_timestamp AS update_timestamp
  FROM
    `moz-fx-data-shared-prod.ctms_braze.ctms_emails` AS emails
  LEFT JOIN
    `moz-fx-data-shared-prod.ctms_braze.ctms_fxa` AS fxa
    ON emails.email_id = fxa.email_id
)
SELECT
  emails.external_id AS external_id,
  emails.email AS email,
  emails.email_subscribe AS email_subscribe,
  emails.mailing_country AS mailing_country,
  emails.basket_token AS basket_token,
  emails.email_lang AS email_lang,
  emails.fxa_id_sha256 AS fxa_id_sha256,
  emails.has_fxa AS has_fxa,
  emails.fxa_primary_email AS fxa_primary_email,
  emails.fxa_lang AS fxa_lang,
  emails.fxa_first_service AS fxa_first_service,
  emails.fxa_created_at AS fxa_created_at,
  emails.create_timestamp AS create_timestamp,
  emails.update_timestamp AS update_timestamp,
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
  AND emails.has_opted_out_of_email = FALSE -- has not opted out of all newsletters
  AND (
    fxa.account_deleted = FALSE -- has not deleted FxA
    OR fxa.account_deleted IS NULL
  )
  AND (
    EXISTS(
      SELECT
        1
      FROM
        `moz-fx-data-shared-prod.ctms_braze.ctms_newsletters` AS newsletters
      INNER JOIN
        `moz-fx-data-shared-prod.braze_derived.subscriptions_map_v1` AS map
        ON newsletters.name = map.braze_subscription_name
      WHERE
        newsletters.email_id = emails.external_id
        AND newsletters.subscribed = TRUE
    )
    OR EXISTS(
      SELECT
        1
      FROM
        `moz-fx-data-shared-prod.ctms_braze.ctms_waitlists` AS waitlists
      INNER JOIN
        `moz-fx-data-shared-prod.braze_derived.subscriptions_map_v1` AS map
        ON IF(
          waitlists.name = "vpn",
          "guardian-vpn-waitlist",
          CONCAT(waitlists.name, "-waitlist")
        ) = map.braze_subscription_name
      WHERE
        waitlists.email_id = emails.external_id
        AND waitlists.subscribed = TRUE
    )
  )
