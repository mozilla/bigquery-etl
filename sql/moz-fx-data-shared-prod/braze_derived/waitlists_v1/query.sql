SELECT
  email_id AS external_id,
  ARRAY_AGG(
    STRUCT(
      name AS waitlist_name,
      LOWER(JSON_EXTRACT_SCALAR(fields, '$.geo')) AS waitlist_geo,
      LOWER(JSON_EXTRACT_SCALAR(fields, '$.platform')) AS waitlist_platform,
      LOWER(source) AS waitlist_source,
      waitlists.create_timestamp AS create_timestamp,
      waitlists.subscribed AS subscribed,
      waitlists.unsub_reason AS unsub_reason,
      waitlists.update_timestamp AS update_timestamp
    )
    ORDER BY
      waitlists.update_timestamp,
      waitlists.create_timestamp,
      waitlists.name
  ) AS waitlists
FROM
  `moz-fx-data-shared-prod.ctms_braze.ctms_waitlists` AS waitlists
INNER JOIN
  `moz-fx-data-shared-prod.braze_derived.users_v1` AS users
  ON users.external_id = waitlists.email_id
GROUP BY
  email_id
HAVING
  LOGICAL_OR(subscribed) = TRUE;
