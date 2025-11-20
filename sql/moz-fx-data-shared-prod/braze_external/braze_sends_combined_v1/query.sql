WITH sends AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_sends_v1`
  WHERE
    DATE(send_time) = DATE(TIMESTAMP_SUB(@click_time, INTERVAL 30 DAY))
),
opens AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_opens_v1`
  WHERE
    DATE(open_time) = DATE(TIMESTAMP_SUB(@click_time, INTERVAL 30 DAY))
),
clicks AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_clicks_v1`
  WHERE
    DATE(click_time) = @click_time
)
SELECT
  clicks.*,
  sends.send_time,
  sends.message_extras,
  opens.open_id,
  opens.machine_open
FROM
  clicks
LEFT JOIN
  sends
  ON clicks.dispatch_id = sends.dispatch_id
LEFT JOIN
  opens
  ON clicks.dispatch_id = opens.dispatch_id
