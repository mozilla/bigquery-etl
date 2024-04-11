SELECT
  email_id AS external_id,
  ARRAY_AGG(
    STRUCT(
      name AS waitlist_name,
      JSON_EXTRACT_SCALAR(fields, '$.geo') AS waitlist_geo,
      JSON_EXTRACT_SCALAR(fields, '$.platform') AS waitlist_platform,
      source AS waitlist_source,
      create_timestamp,
      subscribed,
      unsub_reason,
      update_timestamp
    )
  ) AS waitlists,
  @submission_date AS last_modified_timestamp,
  DATE(@submission_date) AS last_modified_date
FROM
  `moz-fx-data-shared-prod.ctms_braze.ctms_waitlists`
WHERE
  update_timestamp > (
  SELECT
    MAX(last_modified_timestamp)
  FROM
    `moz-fx-data-shared-prod.braze_derived.newsletters_v1` )
GROUP BY
  email_id;
