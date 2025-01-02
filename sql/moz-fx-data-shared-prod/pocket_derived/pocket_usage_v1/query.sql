WITH pocket_activity AS (
  SELECT
    DATE(submission_timestamp) AS events_submission_date,
    normalized_channel AS events_channel,
    event_name,
    COUNT(DISTINCT client_info.client_id) AS event_client_count,
  FROM
    `moz-fx-data-shared-prod.fenix.events_unnested` AS events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'pocket'
    AND metadata.isp.name != 'BrowserStack' -- Removal of known bots. It aims to filter out the data coming from the bots.
    AND ping_info.reason != 'startup' -- Removal of dirty pings
  GROUP BY
    1,
    2,
    3
),
channel_users AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_channel AS channel,
    COUNT(DISTINCT client_info.client_id) AS total_client_count,
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metadata.isp.name != 'BrowserStack' -- Removal of known bots. It aims to filter out the data coming from the bots.
    AND ping_info.reason != 'dirty_startup' -- Removal of dirty pings
  GROUP BY
    1,
    2
)
SELECT
  submission_date,
  channel,
  event_name,
  event_client_count,
  total_client_count,
  (event_client_count / total_client_count) * 100 AS incidence_percentage
FROM
  channel_users AS a
LEFT JOIN
  pocket_activity AS b
  ON submission_date = events_submission_date
  AND channel = events_channel
ORDER BY
  1,
  2,
  3
