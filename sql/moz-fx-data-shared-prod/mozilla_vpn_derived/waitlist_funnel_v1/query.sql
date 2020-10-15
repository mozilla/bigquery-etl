WITH send_ids AS (
  SELECT
    SendID,
    ARRAY_AGG(DISTINCT URL) AS URLs,
  FROM
    `mozilla-cdp-prod`.sfmc.clicks
  WHERE
    URL LIKE "%://vpn.mozilla.org%"
  GROUP BY
    SendID
),
jobs AS (
  SELECT
    SendID,
    SentTime,
    URLs,
  FROM
    `mozilla-cdp-prod`.sfmc.sendjobs
  INNER JOIN
    send_ids
  USING
    (SendID)
)
SELECT
  (
    SELECT
      COUNT(DISTINCT EmailAddress),
    FROM
      `mozilla-cdp-prod`.sfmc.sent
    JOIN
      jobs
    USING
      (SendID)
    WHERE
      TIMESTAMP_DIFF(EventDate, SentTime, DAY) < 14
  ) AS sent,
  (
    SELECT
      COUNT(DISTINCT EmailAddress),
    FROM
      `mozilla-cdp-prod`.sfmc.bounces
    JOIN
      jobs
    USING
      (SendID)
    WHERE
      TIMESTAMP_DIFF(EventDate, SentTime, DAY) < 14
  ) AS bounced,
  (
    SELECT
      COUNT(DISTINCT EmailAddress),
    FROM
      `mozilla-cdp-prod`.sfmc.opens
    JOIN
      jobs
    USING
      (SendID)
    WHERE
      TIMESTAMP_DIFF(EventDate, SentTime, DAY) < 14
  ) AS opened,
  (
    SELECT
      COUNT(DISTINCT EmailAddress),
    FROM
      `mozilla-cdp-prod`.sfmc.unsubs
    JOIN
      jobs
    USING
      (SendID)
    WHERE
      TIMESTAMP_DIFF(EventDate, SentTime, DAY) < 14
  ) AS unsubscribed,
  (
    SELECT
      COUNT(DISTINCT EmailAddress),
    FROM
      `mozilla-cdp-prod`.sfmc.clicks
    JOIN
      jobs
    USING
      (SendID)
    WHERE
      TIMESTAMP_DIFF(EventDate, SentTime, DAY) < 14
      AND URL IN UNNEST(URLs)
  ) AS clicked,
  (
    SELECT
      COUNT(DISTINCT EmailAddress),
    FROM
      `mozilla-cdp-prod`.sfmc.clicks
    JOIN
      jobs
    USING
      (SendID)
    JOIN
      `moz-fx-data-shared-prod`.mozilla_vpn_external.users_v1
    ON
      (EmailAddress = email AND DATE(created_at) >= DATE(EventDate))
    WHERE
      TIMESTAMP_DIFF(created_at, SentTime, DAY) < 14
      AND URL IN UNNEST(URLs)
  ) AS signed_up,
