CREATE OR REPLACE TABLE
  plans_v1
PARTITION BY
  DATE(event_timestamp)
CLUSTER BY
  id
AS
SELECT
  id,
  ARRAY_AGG(event ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  plan_events AS event
GROUP BY
  id
