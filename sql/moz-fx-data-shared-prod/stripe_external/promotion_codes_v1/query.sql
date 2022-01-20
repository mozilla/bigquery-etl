WITH events AS (
  SELECT
    `data`.promotion_code.id,
    created AS event_timestamp,
    `data`.promotion_code.* EXCEPT (id),
  FROM
    events_v1
  WHERE
    `data`.promotion_code IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    promotion_codes_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
