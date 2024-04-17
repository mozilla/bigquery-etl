WITH ctms_emails AS (
  SELECT
    e.email_id AS external_id,
    e.primary_email AS email,
    e.mailing_country,
    CASE
      WHEN e.double_opt_in = TRUE
        THEN 'opted_in'
      WHEN e.double_opt_in = FALSE
        AND e.has_opted_out_of_email = FALSE
        THEN 'subscribed'
      ELSE 'unsubscribed'
    END AS email_subscribe,
    e.basket_token,
    e.email_lang,
    COALESCE(TO_HEX(SHA256(f.fxa_id)), TO_HEX(SHA256(fxa.fxa_id))) AS fxa_id_sha256,
    f.primary_email AS fxa_primary_email,
    f.lang AS fxa_lang,
    f.first_service,
    e.create_timestamp,
    e.update_timestamp
  FROM
    `moz-fx-data-shared-prod.ctms_braze.ctms_emails` e
  LEFT JOIN
    `moz-fx-data-shared-prod.ctms_braze.ctms_fxa` f
    ON e.email_id = f.email_id
  LEFT JOIN
    `moz-fx-data-shared-prod.ctms.ctms_fxa` fxa
    ON e.email_id = fxa.email_id
),
exclusions AS (
  SELECT
    e.external_id,
    e.email,
    e.mailing_country,
    e.email_subscribe,
    e.basket_token,
    e.email_lang,
    e.fxa_id_sha256,
    e.fxa_primary_email,
    e.fxa_lang,
    e.first_service,
    e.create_timestamp,
    e.update_timestamp
  FROM
    ctms_emails e
  LEFT JOIN
    `moz-fx-data-shared-prod.braze_derived.suppressions_v1` s
    ON LOWER(e.email) = s.email
  LEFT JOIN
    `moz-fx-data-shared-prod.ctms_braze.ctms_mofo` mofo
    ON e.external_id = mofo.email_id
  LEFT JOIN
    `moz-fx-data-shared-prod.braze_derived.newsletters_v1` n
    ON n.external_id = e.external_id
  LEFT JOIN
    `moz-fx-data-shared-prod.braze_derived.waitlists_v1` w
    ON w.external_id = e.external_id
  LEFT JOIN
    `moz-fx-data-shared-prod.braze_derived.products_v1` p
    ON p.fxa_id_sha256 = e.fxa_id_sha256
  WHERE
    s.email IS NULL -- exclude users on suppression list
    AND e.email IS NOT NULL
    -- exclude users from mofo not associated w/ newsletters, waitlists, or products
    AND (
      mofo.email_id IS NULL
      OR ARRAY_LENGTH(n.newsletters) > 0
      OR ARRAY_LENGTH(w.waitlists) > 0
      OR ARRAY_LENGTH(p.products) > 0
    )
)
SELECT
  external_id,
  email,
  mailing_country,
  email_subscribe,
  basket_token,
  email_lang,
  fxa_id_sha256,
  fxa_primary_email,
  fxa_lang,
  first_service,
  create_timestamp,
  update_timestamp
FROM
  exclusions;
