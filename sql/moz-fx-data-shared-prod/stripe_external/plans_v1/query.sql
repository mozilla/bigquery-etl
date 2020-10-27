WITH events AS (
  SELECT
    `data`.plan.id,
    created AS event_timestamp,
    `data`.plan.* EXCEPT (id),
  FROM
    events_v1
  WHERE
    `data`.plan IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    plans_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
