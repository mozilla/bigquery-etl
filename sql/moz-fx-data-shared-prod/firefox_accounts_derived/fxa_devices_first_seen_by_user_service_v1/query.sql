WITH fxa_events AS (
  SELECT
    `timestamp`,
    user_id,
    IF(service IS NULL AND event_type = 'fxa_activity - cert_signed', 'sync', service) AS service,
    device_id,
    os_name,
    flow_id,
    event_type,
    country,
    `language`,
    entrypoint,
    utm_term,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    ua_version,
    ua_browser,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts.fxa_content_auth_oauth_events`
  WHERE
    DATE(`timestamp`)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND @submission_date
),
entrypoints AS (
  SELECT DISTINCT
    flow_id,
    entrypoint
  FROM
    fxa_events
  WHERE
    entrypoint IS NOT NULL
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY flow_id ORDER BY `timestamp` ASC) = 1
),
utms AS (
  SELECT DISTINCT
    flow_id,
    FIRST_VALUE(utm_term) OVER (_window) AS utm_term,
    FIRST_VALUE(utm_medium) OVER (_window) AS utm_medium,
    FIRST_VALUE(utm_source) OVER (_window) AS utm_source,
    FIRST_VALUE(utm_campaign) OVER (_window) AS utm_campaign,
    FIRST_VALUE(utm_content) OVER (_window) AS utm_content,
  FROM
    fxa_events
  WHERE
    flow_id IS NOT NULL
  WINDOW
    _window AS (
      PARTITION BY
        flow_id
      ORDER BY
        `timestamp` ASC
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    )
),
new_user_service_device_entries AS (
  SELECT DISTINCT
    flow_id,
    device_id,
    `timestamp`,
    user_id,
    service,
    os_name,
    country,
    `language`,
    ua_version,
    ua_browser,
  FROM
    fxa_events
  WHERE
    DATE(`timestamp`) = @submission_date
    AND flow_id IS NOT NULL
    AND ((event_type IN ('fxa_login - complete', 'fxa_reg - complete') AND service IS NOT NULL))
    AND CONCAT(user_id, service, device_id) NOT IN (
      SELECT
        CONCAT(user_id, service, device_id)
      FROM
        `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_devices_first_seen_by_user_service_v1`
      WHERE
        DATE(first_seen_date) < @submission_date
    )
),
first_values AS (
  SELECT DISTINCT
    device_id,
    FIRST_VALUE(flow_id) OVER (_window) AS first_flow_id,
    FIRST_VALUE(`timestamp`) OVER (_window) AS first_seen_date,
    ANY_VALUE(user_id) OVER (_window) AS user_id,
    ANY_VALUE(service) OVER (_window) AS service,
    ANY_VALUE(os_name) OVER (_window) AS os_name,
    ANY_VALUE(country) OVER (_window) AS country,
    ANY_VALUE(`language`) OVER (_window) AS `language`,
    ANY_VALUE(ua_version) OVER (_window) AS ua_version,
    ANY_VALUE(ua_browser) OVER (_window) AS ua_browser,
  FROM
    new_user_service_device_entries
  WINDOW
    _window AS (
      PARTITION BY
        device_id
      ORDER BY
        `timestamp` ASC
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    )
)
SELECT
  first_values.*,
  entrypoints.* EXCEPT (flow_id),
  utms.* EXCEPT (flow_id),
FROM
  first_values
LEFT JOIN
  entrypoints
ON
  first_values.first_flow_id = entrypoints.flow_id
LEFT JOIN
  utms
ON
  first_values.first_flow_id = utms.flow_id
WHERE
  user_id IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      user_id,
      service,
      device_id
    ORDER BY
      first_seen_date ASC
  ) = 1  -- this could be partitioned by first_flow_id to handle multiple device_ids being attached to a single flow_id
