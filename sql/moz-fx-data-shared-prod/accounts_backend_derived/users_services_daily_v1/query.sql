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

WITH fxa_events AS (
  SELECT
    submission_timestamp,
    metrics.string.account_user_id_sha256 AS user_id,
    IF(
      metrics.string.relying_party_oauth_client_id = '',
      metrics.string.relying_party_service,
      metrics.string.relying_party_oauth_client_id
    ) AS service,
    client_info.os AS os_name,
    client_info.os_version AS os_version,
    client_info.app_display_version AS app_version,
    metrics.string.session_flow_id AS flow_id,
    metrics.string.session_entrypoint AS entrypoint,
    metrics.string.event_name AS event_name,
    metadata.geo.country AS country,
    client_info.locale AS language,
    metadata.user_agent.version AS ua_version,
    metadata.user_agent.browser AS ua_browser,
    metrics.string.utm_term AS utm_term,
    metrics.string.utm_medium AS utm_medium,
    metrics.string.utm_source AS utm_source,
    metrics.string.utm_campaign AS utm_campaign,
    metrics.string.utm_content AS utm_content,
  FROM
    `accounts_backend.accounts_events`
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND @submission_date
    AND metrics.string.event_name IN (
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
    user_id,
    service,
    udf.mode_last(ARRAY_AGG(country) OVER w1) AS country,
    udf.mode_last(ARRAY_AGG(LANGUAGE) OVER w1) AS language,
    udf.mode_last(ARRAY_AGG(app_version) OVER w1) AS app_version,
    udf.mode_last(ARRAY_AGG(os_name) OVER w1) AS os_name,
    udf.mode_last(ARRAY_AGG(os_version) OVER w1) AS os_version,
    udf.mode_last(ARRAY_AGG(ua_version) OVER w1) AS ua_version,
    udf.mode_last(ARRAY_AGG(ua_browser) OVER w1) AS ua_browser,
    udf_contains_tier1_country(ARRAY_AGG(country) OVER w1) AS seen_in_tier1_country,
    LOGICAL_OR(event_name = 'reg_complete') OVER w1 AS registered,
    ARRAY_AGG(event_name) OVER w1 AS service_events,
  FROM
    fxa_events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND user_id != ''
    AND service != ''
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id,
        service,
        DATE(submission_timestamp)
      ORDER BY
        submission_timestamp
    ) = 1
  WINDOW
    w1 AS (
      PARTITION BY
        user_id,
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
  windowed.user_id,
  oa.name AS service,
  windowed.country,
  windowed.language,
  windowed.app_version,
  windowed.os_name,
  windowed.os_version,
  windowed.ua_version,
  windowed.ua_browser,
  windowed.seen_in_tier1_country,
  windowed.registered,
  windowed.service_events,
FROM
  windowed
JOIN
  `accounts_db.fxa_oauth_clients` AS oa
  ON windowed.service = oa.id
WHERE
  user_id != ''
  AND service != ''
