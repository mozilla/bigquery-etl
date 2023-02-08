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
    user_id,
    `service`,
    app_version,
    os_name,
    os_version,
    country,
    `language`,
    ua_version,
    ua_browser,
    user_service_first_daily_flow_info,
    user_service_utm_info,
    registered,
  FROM
    `firefox_accounts_derived.fxa_users_services_daily_v1`
  WHERE
    submission_date = @submission_date
    AND contains_qualified_fxa_activity_event(service_events, `service`)
),
existing_entries AS (
  SELECT
    user_id,
    `service`,
  FROM
    `firefox_accounts_derived.fxa_users_services_first_seen_v1`
  WHERE
    DATE(first_service_timestamp) < @submission_date
)
SELECT
  new_records.user_id,
  new_records.`service`,
  new_records.registered AS did_register,
  new_records.os_name AS first_service_os_name,
  new_records.os_version AS first_service_os_version,
  new_records.app_version AS first_service_app_version,
  new_records.country AS first_service_country,
  new_records.`language` AS first_service_language,
  new_records.ua_version AS first_service_ua_version,
  new_records.ua_browser AS first_service_ua_browser,
  new_records.user_service_first_daily_flow_info.flow_id AS first_service_flow,
  new_records.user_service_first_daily_flow_info.`timestamp` AS first_service_flow_timestamp,
  new_records.user_service_first_daily_flow_info.entrypoint AS first_service_flow_entrypoint,
  new_records.user_service_utm_info.utm_term AS first_service_flow_utm_term,
  new_records.user_service_utm_info.utm_medium AS first_service_flow_utm_medium,
  new_records.user_service_utm_info.utm_source AS first_service_flow_utm_source,
  new_records.user_service_utm_info.utm_campaign AS first_service_flow_utm_campaign,
  new_records.user_service_utm_info.utm_content AS first_service_flow_utm_content,
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
