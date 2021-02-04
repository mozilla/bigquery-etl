WITH events AS (
  SELECT
    `data`.payout.id,
    created AS event_timestamp,
    `data`.payout.* EXCEPT (id),
  FROM
    events_v1
  WHERE
    `data`.payout IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    payouts_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
