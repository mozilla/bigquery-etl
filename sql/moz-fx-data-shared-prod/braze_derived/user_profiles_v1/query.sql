SELECT
  u.email_id AS external_id,
  u.email,
  u.mailing_country,
  u.email_subscribe,
  u.basket_token,
  u.email_lang,
  u.fxa_id_sha256,
  u.fxa_primary_email,
  u.fxa_lang,
  u.first_service,
  u.SOURCE AS user_source,
  p.products,
  s.newsletters,
  w.waitlists
FROM
  braze_external.dev_users u
LEFT JOIN
  braze_external.dev_products p
ON
  u.fxa_id_sha256 = p.fxa_id_sha256
LEFT JOIN
  braze_external.dev_subscriptions s
ON
  u.email_id = s.external_id
LEFT JOIN
  braze_external.dev_waitlists w
ON
  u.email_id = w.external_id
LEFT JOIN
  `moz-fx-data-shared-prod.ctms_braze.ctms_mofo` mofo
ON
  u.email_id = mofo.email_id
WHERE
  -- Filter out MoFo users who are not subscribed to a newsletter,
  -- on a waitlist, or consuming a product
  mofo.email_id IS NULL -- Include users not in mofo by default
  OR ( ARRAY_LENGTH(s.newsletters) > 0 -- Has at least one newsletter subscription
    OR ARRAY_LENGTH(w.waitlists) > 0 -- Is on at least one waitlist
    OR ARRAY_LENGTH(p.products) > 0 -- Has at least one product
    );
