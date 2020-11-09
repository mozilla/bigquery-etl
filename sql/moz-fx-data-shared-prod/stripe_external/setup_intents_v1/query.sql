WITH events AS (
  SELECT
    `data`.setup_intent.id,
    created AS event_timestamp,
    `data`.setup_intent.* EXCEPT (id),
  FROM
    events_v1
  WHERE
    `data`.setup_intent IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    setup_intents_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
