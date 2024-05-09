SELECT
  users.external_id,
  users.email,
  users.mailing_country,
  users.email_subscribe,
  users.basket_token,
  users.email_lang,
  users.create_timestamp,
  users.update_timestamp,
  users.fxa_id_sha256,
  users.has_fxa,
  users.fxa_primary_email,
  users.fxa_lang,
  users.fxa_first_service,
  users.fxa_created_at,
  newsletters.newsletters,
  waitlists.waitlists,
  products.products
FROM
  `moz-fx-data-shared-prod.braze_derived.users_v1` AS users
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.newsletters_v1` AS newsletters
  ON users.external_id = newsletters.external_id
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.waitlists_v1` AS waitlists
  ON users.external_id = waitlists.external_id
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.products_v1` AS products
  ON users.external_id = products.external_id;
