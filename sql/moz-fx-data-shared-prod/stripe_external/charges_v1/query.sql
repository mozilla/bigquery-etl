WITH events AS (
  SELECT
    `data`.charge.id,
    created AS event_timestamp,
    `data`.charge.* EXCEPT (id),
  FROM
    events_v1
  WHERE
    `data`.charge IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    charges_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
