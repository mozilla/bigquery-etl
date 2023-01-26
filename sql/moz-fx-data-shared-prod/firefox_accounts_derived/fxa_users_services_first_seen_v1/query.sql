-- contains_qualified_fxa_activity_event
-- Expects an array of event_types and service name
-- Iterates and checks if an event_type, service combination
-- should be considered when determining if this is the first
-- time it has been observed.
-- Example usage:
-- contains_qualified_fxa_activity_event(['fxa_activity'], NULL),  # returns TRUE
-- contains_qualified_fxa_activity_event(['fxa_activity'], 'firefox-ios'),  # returns TRUE
-- contains_qualified_fxa_activity_event(['fxa_login - complete'], NULL),  # returns FALSE
-- contains_qualified_fxa_activity_event(['fxa_login - complete'], 'firefox-ios'),  # returns TRUE
CREATE TEMP FUNCTION contains_qualified_fxa_activity_event(events ANY TYPE, `service` ANY TYPE) AS (
  EXISTS(
    SELECT
      event_type
    FROM
      UNNEST(events) AS event_type
    WHERE
      event_type IN ('fxa_login - complete', 'fxa_reg - complete')
      OR (event_type LIKE 'fxa_activity%')
  )
);

WITH fxa_users_services_daily_events AS (
  SELECT
    submission_date,
    `timestamp`,
    flow_id,
    user_id,
    `service`,
    app_version,
    os_name,
    os_version,
    country,
    entrypoint,
    `language`,
    ua_version,
    ua_browser,
    utm_term,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    registered,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts.fxa_users_services_daily`
  WHERE
    submission_date = @submission_date
    AND user_id IS NOT NULL
    AND `service` IS NOT NULL
    AND contains_qualified_fxa_activity_event(service_events, `service`)
),
existing_entries AS (
  SELECT
    user_id,
    `service`,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts.fxa_users_services_first_seen`
  WHERE
    DATE(first_service_timestamp) < @submission_date
)
SELECT
  new_records.`timestamp` AS first_service_timestamp,
  new_records.flow_id AS first_service_flow,
  new_records.user_id,
  new_records.`service`,
  new_records.registered AS did_register,
  new_records.os_name AS first_service_os,
  new_records.os_version,
  new_records.app_version,
  new_records.country AS first_service_country,
  new_records.entrypoint,
  new_records.`language`,
  new_records.ua_version,
  new_records.ua_browser,
  new_records.utm_term,
  new_records.utm_medium,
  new_records.utm_source,
  new_records.utm_campaign,
  new_records.utm_content,
  -- ROW_NUMBER() OVER (
  --   PARTITION BY
  --     new_records.user_id
  --   ORDER BY
  --     new_records.`timestamp`
  -- ) AS service_number -- TODO: what is this field? Is this needed and how is it used?
  NULL AS service_number,
FROM
  fxa_users_services_daily_events AS new_records
FULL OUTER JOIN
  existing_entries
USING
  (user_id, `service`)
WHERE
  existing_entries.user_id IS NULL
  AND existing_entries.`service` IS NULL
