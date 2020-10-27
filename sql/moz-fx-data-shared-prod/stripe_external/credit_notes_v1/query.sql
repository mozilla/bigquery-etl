WITH events AS (
  SELECT
    `data`.credit_note.id,
    created AS event_timestamp,
    `data`.credit_note.* EXCEPT (id),
  FROM
    events_v1
  WHERE
    `data`.credit_note IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
  FROM
    credit_notes_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
