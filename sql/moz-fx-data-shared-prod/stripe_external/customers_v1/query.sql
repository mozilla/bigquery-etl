WITH events AS (
  SELECT
    `data`.customer.id,
    created AS event_timestamp,
    `data`.customer.* EXCEPT (id),
  FROM
    events_v1
  WHERE
    `data`.customer IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    customers_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
