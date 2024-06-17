-- Compares users from the previous run to detect changes to sync
SELECT
  COALESCE(current_users.external_id, previous_users.external_id) AS external_id,
  CASE
    WHEN current_users.external_id IS NULL
      THEN 'Deleted'
    WHEN previous_users.external_id IS NULL
      THEN 'New'
    WHEN NOT (
        current_users.email = previous_users.email
        AND current_users.mailing_country = previous_users.mailing_country
        AND current_users.email_subscribe = previous_users.email_subscribe
        AND current_users.basket_token = previous_users.basket_token
        AND current_users.email_lang = previous_users.email_lang
        AND current_users.create_timestamp = previous_users.create_timestamp
        AND current_users.update_timestamp = previous_users.update_timestamp
        AND current_users.fxa_id_sha256 = previous_users.fxa_id_sha256
        AND current_users.has_fxa = previous_users.has_fxa
        AND current_users.fxa_primary_email = previous_users.fxa_primary_email
        AND current_users.fxa_lang = previous_users.fxa_lang
        AND current_users.fxa_first_service = previous_users.fxa_first_service
        AND current_users.fxa_created_at = previous_users.fxa_created_at
      )
      THEN 'Changed'
  END AS status,
  COALESCE(current_users.email, previous_users.email) AS email,
  COALESCE(current_users.mailing_country, previous_users.mailing_country) AS mailing_country,
  COALESCE(current_users.email_subscribe, previous_users.email_subscribe) AS email_subscribe,
  COALESCE(current_users.basket_token, previous_users.basket_token) AS basket_token,
  COALESCE(current_users.email_lang, previous_users.email_lang) AS email_lang,
  COALESCE(current_users.create_timestamp, previous_users.create_timestamp) AS create_timestamp,
  COALESCE(current_users.update_timestamp, previous_users.update_timestamp) AS update_timestamp,
  COALESCE(current_users.has_fxa, previous_users.has_fxa) AS has_fxa,
  COALESCE(current_users.fxa_primary_email, previous_users.fxa_primary_email) AS fxa_primary_email,
  COALESCE(current_users.fxa_lang, previous_users.fxa_lang) AS fxa_lang,
  COALESCE(current_users.fxa_first_service, previous_users.fxa_first_service) AS fxa_first_service,
  COALESCE(current_users.fxa_created_at, previous_users.fxa_created_at) AS fxa_created_at
FROM
  `moz-fx-data-shared-prod.braze_derived.users_v1` current_users
FULL OUTER JOIN
  `moz-fx-data-shared-prod.braze_external.users_previous_day_snapshot_v1` previous_users --change back to v1 after run tomorrow
  ON current_users.external_id = previous_users.external_id
WHERE
  current_users.external_id IS NULL  -- deleted rows
  OR previous_users.external_id IS NULL -- new rows
  OR NOT (
    current_users.email = previous_users.email
    AND current_users.mailing_country = previous_users.mailing_country
    AND current_users.email_subscribe = previous_users.email_subscribe
    AND current_users.basket_token = previous_users.basket_token
    AND current_users.email_lang = previous_users.email_lang
    AND current_users.create_timestamp = previous_users.create_timestamp
    AND current_users.update_timestamp = previous_users.update_timestamp
    AND current_users.has_fxa = previous_users.has_fxa
    AND current_users.fxa_primary_email = previous_users.fxa_primary_email
    AND current_users.fxa_lang = previous_users.fxa_lang
    AND current_users.fxa_first_service = previous_users.fxa_first_service
    AND current_users.fxa_created_at = previous_users.fxa_created_at
  ); -- changed rows
