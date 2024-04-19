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
    ORDER BY
      update_timestamp,
      create_timestamp,
      name
  ) AS newsletters
FROM
  `moz-fx-data-shared-prod.ctms_braze.ctms_newsletters`
GROUP BY
  email_id;
