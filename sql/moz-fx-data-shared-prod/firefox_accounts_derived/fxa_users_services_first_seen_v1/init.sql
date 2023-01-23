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
      (event_type IN ('fxa_login - complete', 'fxa_reg - complete') AND `service` IS NOT NULL)
      OR (event_type LIKE 'fxa_activity%')
  )
);

CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_first_seen_v1`
PARTITION BY
  DATE(first_service_timestamp)
CLUSTER BY
  service,
  user_id
AS
SELECT
  submission_date,
  `timestamp` AS first_service_timestamp,
  flow_id AS first_service_flow,
  user_id,
  `service`,
  events_observed,
  app_version,
  os_name AS first_service_os,
  os_version,
  country AS first_service_country,
  entrypoint,
  `language`,
  ua_version,
  ua_browser,
  utm_term,
  utm_medium,
  utm_source,
  utm_campaign,
  utm_content,
  registered AS did_register,
  -- ROW_NUMBER() OVER (
  --   PARTITION BY
  --     new_records.user_id
  --   ORDER BY
  --     new_records.`timestamp`
  -- ) AS service_number -- TODO: what is this field? Is this needed and how is it used?
  NULL AS service_number,
FROM
  `moz-fx-data-shared-prod.firefox_accounts.fxa_users_services_daily`
WHERE
  user_id IS NOT NULL
  AND `service` IS NOT NULL
  AND contains_qualified_fxa_activity_event(events_observed, `service`)
QUALIFY
  ROW_NUMBER() OVER w1_unframed = 1
WINDOW
  -- We must provide a modified window for ROW_NUMBER which cannot accept a frame clause.
  w1_unframed AS (
    PARTITION BY
      user_id,
      service
    ORDER BY
      `timestamp` ASC
  )
