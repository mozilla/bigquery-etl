SELECT
  email_id,
  ARRAY_AGG( STRUCT( name AS newsletter_name,
      subscribed,
      lang AS newsletter_lang,
      create_timestamp,
      update_timestamp ) ) AS newsletters,
  @submission_date AS last_modified_timestamp,
  DATE(@submission_date) AS last_modified_date
FROM
  `moz-fx-data-shared-prod.ctms_braze.ctms_newsletters`
GROUP BY
  email_id;
