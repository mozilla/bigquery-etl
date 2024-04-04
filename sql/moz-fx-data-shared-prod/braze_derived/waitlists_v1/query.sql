SELECT
  w.email_id AS external_id,
  ARRAY_AGG(
    STRUCT(
      w.name AS waitlist_name,
      JSON_EXTRACT_SCALAR(w.fields, '$.geo') AS waitlist_geo,
      JSON_EXTRACT_SCALAR(w.fields, '$.platform') AS waitlist_platform,
      w.source AS waitlist_source,
      w.create_timestamp,
      w.subscribed,
      w.unsub_reason,
      w.update_timestamp
    )
  ) AS waitlists
FROM
  `moz-fx-data-shared-prod.ctms_braze.ctms_waitlists` AS w
GROUP BY
  w.email_id;
