CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.fb_desktop_conversions`
AS
SELECT
  submission_date,
  activity_date,
  (activity_date).TIMESTAMP().UNIX_SECONDS() AS activity_unix_timestamp,
  -- Expected fbc format: "fbc.{subdomain_index}.{creation_time}.{fbclid}"
  CONCAT(
    "fb.0.",
    CAST((ga_event_timestamp).TIMESTAMP_MICROS().UNIX_SECONDS() AS STRING),
    ".",
    fbclid
  ) AS fbc,
  conversion_name,
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.fb_desktop_conversions_v1`
WHERE
  ga_country IN ("United States", "India")
