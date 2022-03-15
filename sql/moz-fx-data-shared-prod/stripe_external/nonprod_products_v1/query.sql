WITH events AS (
  SELECT
    `data`.product.id,
    created AS event_timestamp,
    `data`.product.* EXCEPT (id),
  FROM
    nonprod_events_v1
  WHERE
    `data`.product IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    nonprod_products_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
