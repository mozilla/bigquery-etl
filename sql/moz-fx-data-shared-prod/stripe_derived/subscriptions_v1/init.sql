CREATE OR REPLACE TABLE
  subscriptions_v1
PARTITION BY
  DATE(event_timestamp)
CLUSTER BY
  plan
AS
SELECT
  id,
  ARRAY_AGG(event ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  subscription_events AS event
GROUP BY
  id
