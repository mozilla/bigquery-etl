SELECT
  u.external_id,
  u.email,
  u.mailing_country,
  u.email_subscribe,
  u.basket_token,
  u.email_lang,
  u.create_timestamp,
  u.update_timestamp,
  u.fxa_id_sha256,
  u.fxa_primary_email,
  u.fxa_lang,
  u.first_service,
  n.newsletters,
  w.waitlists,
  p.products,
  @submission_date AS last_modified_timestamp,
  DATE(@submission_date) AS last_modified_date
FROM
  `moz-fx-data-shared-prod.braze_derived.users_v1` u
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.newsletters_v1` n
  ON u.external_id = n.external_id
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.waitlists_v1` w
  ON u.external_id = w.external_id
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.products_v1` p
  ON u.fxa_id_sha256 = p.fxa_id_sha256;
