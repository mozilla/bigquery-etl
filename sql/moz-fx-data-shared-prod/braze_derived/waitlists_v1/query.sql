SELECT
  email_id AS external_id,
  ARRAY_AGG(
    STRUCT(
      name AS waitlist_name,
      JSON_EXTRACT_SCALAR(fields, '$.geo') AS waitlist_geo,
      JSON_EXTRACT_SCALAR(fields, '$.platform') AS waitlist_platform,
      SOURCE AS waitlist_source,
      create_timestamp,
      subscribed,
      unsub_reason,
      update_timestamp
    )
  ) AS waitlists
FROM
  `moz-fx-data-shared-prod.ctms_braze.ctms_waitlists`
GROUP BY
  email_id;
