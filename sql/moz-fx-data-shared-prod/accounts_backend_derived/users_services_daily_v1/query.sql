CREATE TEMP FUNCTION udf_contains_tier1_country(x ANY TYPE) AS ( --
  EXISTS(
    SELECT
      country
    FROM
      UNNEST(x) AS country
    WHERE
      country IN ( --
        'United States',
        'France',
        'Germany',
        'United Kingdom',
        'Canada'
      )
  )
);

CREATE TEMP FUNCTION count_distinct(arr ANY TYPE) AS (
  (SELECT COUNT(DISTINCT x) FROM UNNEST(arr) AS x)
);

WITH events_unnested AS (
  SELECT
    e.* EXCEPT (events),
    event.timestamp AS event_timestamp,
    CONCAT(event.category, "_", event.name) AS event_name,
    event.extra AS event_extra
  FROM
    `moz-fx-data-shared-prod.accounts_backend.events` AS e
  CROSS JOIN
    UNNEST(e.events) AS event
),
fxa_events AS (
  SELECT
    submission_timestamp,
    metrics.string.account_user_id_sha256 AS user_id_sha256,
    IF(
      metrics.string.relying_party_oauth_client_id = '',
      metrics.string.relying_party_service,
      metrics.string.relying_party_oauth_client_id
    ) AS service,
    metrics.string.session_flow_id AS flow_id,
    metrics.string.session_entrypoint AS entrypoint,
    event_name,
    -- `access_token_checked` events are triggered on traffic from RP backend services and don't have client's geo data
    IF(event_name != 'access_token_checked', metadata.geo.country, NULL) AS country,
    metrics.string.utm_term AS utm_term,
    metrics.string.utm_medium AS utm_medium,
    metrics.string.utm_source AS utm_source,
    metrics.string.utm_campaign AS utm_campaign,
    metrics.string.utm_content AS utm_content,
    metadata.user_agent,
  FROM
    events_unnested
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND @submission_date
    AND event_name IN (
      'access_token_checked',
      'access_token_created',
      -- registration and login events used when deriving the first_seen table
      'reg_complete',
      'login_complete'
    )
),
windowed AS (
  SELECT
    submission_timestamp,
    user_id_sha256,
    service,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(country) OVER w1) AS country,
    udf_contains_tier1_country(ARRAY_AGG(country) OVER w1) AS seen_in_tier1_country,
    LOGICAL_OR(event_name = 'reg_complete') OVER w1 AS registered,
    -- we cannot count distinct here because the window is ordered by submission_timestamp
    ARRAY_AGG(
      CONCAT(
        COALESCE(user_agent.browser, ''),
        '_',
        COALESCE(user_agent.os, ''),
        '_',
        COALESCE(user_agent.version, '')
      )
    ) OVER w1 AS user_agent_devices,
  FROM
    fxa_events
  WHERE
    DATE(submission_timestamp) = @submission_date
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id_sha256,
        service,
        DATE(submission_timestamp)
      ORDER BY
        submission_timestamp
    ) = 1
  WINDOW
    w1 AS (
      PARTITION BY
        user_id_sha256,
        service,
        DATE(submission_timestamp)
      ORDER BY
        submission_timestamp
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    )
)
SELECT
  DATE(@submission_date) AS submission_date,
  windowed.user_id_sha256,
  windowed.service,
  windowed.country,
  windowed.seen_in_tier1_country,
  windowed.registered,
  count_distinct(windowed.user_agent_devices) AS user_agent_device_count,
FROM
  windowed
