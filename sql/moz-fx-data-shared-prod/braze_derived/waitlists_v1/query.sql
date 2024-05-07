SELECT
  email_id AS external_id,
  ARRAY_AGG(
    STRUCT(
      name AS waitlist_name,
      LOWER(JSON_EXTRACT_SCALAR(fields, '$.geo')) AS waitlist_geo,
      LOWER(JSON_EXTRACT_SCALAR(fields, '$.platform')) AS waitlist_platform,
      LOWER(source) AS waitlist_source,
      create_timestamp,
      subscribed,
      unsub_reason,
      update_timestamp
    )
    ORDER BY
      update_timestamp,
      create_timestamp,
      name
  ) AS waitlists
FROM
  `moz-fx-data-shared-prod.ctms_braze.ctms_waitlists`
GROUP BY
  email_id
HAVING
  LOGICAL_OR(subscribed) = TRUE;
