SELECT
  email_id AS external_id,
  ARRAY_AGG(
    STRUCT(
      name AS newsletter_name,
      subscribed,
      lang AS newsletter_lang,
      create_timestamp,
      update_timestamp
    )
  ) AS newsletters,
  CURRENT_TIMESTAMP() AS last_modified_timestamp,
FROM
  `moz-fx-data-shared-prod.ctms_braze.ctms_newsletters`
GROUP BY
  email_id;
