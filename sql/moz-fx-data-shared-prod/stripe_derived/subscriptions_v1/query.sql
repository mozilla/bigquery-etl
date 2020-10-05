WITH _current AS (
  SELECT
    id,
    ARRAY_AGG(event ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
  FROM
    subscription_events AS event
  WHERE
    DATE(event_timestamp) = @date
  GROUP BY
    id
)
SELECT
  IF(
    _previous.id IS NULL
    OR _current.event_timestamp > _previous.event_timestamp,
    _current,
    _previous
  ).*
FROM
  _current
FULL JOIN
  stripe_derived.subscriptions_v1 AS _previous
USING
  (id)
