WITH events AS (
  SELECT
    `data`.invoice.id,
    created AS event_timestamp,
    `data`.invoice.* EXCEPT (id),
  FROM
    events_v1
  WHERE
    `data`.invoice IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    invoices_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
