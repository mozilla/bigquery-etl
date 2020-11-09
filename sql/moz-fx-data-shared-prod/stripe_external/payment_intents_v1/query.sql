WITH events AS (
  SELECT
    `data`.payment_intent.id,
    created AS event_timestamp,
    `data`.payment_intent.* EXCEPT (id),
  FROM
    events_v1
  WHERE
    `data`.payment_intent IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    payment_intents_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
