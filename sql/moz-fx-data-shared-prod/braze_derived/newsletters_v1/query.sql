SELECT
  newsletters.email_id AS external_id,
  ARRAY_AGG(
    STRUCT(
      newsletters.name AS newsletter_name,
      newsletters.subscribed AS subscribed,
      LOWER(newsletters.lang) AS newsletter_lang,
      newsletters.source AS newsletter_source,
      newsletters.create_timestamp AS create_timestamp,
      newsletters.update_timestamp AS update_timestamp
    )
    ORDER BY
      newsletters.update_timestamp,
      newsletters.name
  ) AS newsletters
FROM
  `moz-fx-data-shared-prod.ctms_braze.ctms_newsletters` AS newsletters
INNER JOIN
  `moz-fx-data-shared-prod.braze_derived.users_v1` AS users
  ON users.external_id = newsletters.email_id
GROUP BY
  email_id
HAVING
  LOGICAL_OR(subscribed) = TRUE;
