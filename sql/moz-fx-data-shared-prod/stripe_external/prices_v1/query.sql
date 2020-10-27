WITH events AS (
  SELECT
    `data`.price.id,
    created AS event_timestamp,
    `data`.price.* EXCEPT (id),
  FROM
    events_v1
  WHERE
    `data`.price IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    prices_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
