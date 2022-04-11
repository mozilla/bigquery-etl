-- Invoices frequently get multiple events with the same timestamp, so define a status order based
-- on https://stripe.com/docs/invoicing/overview#workflow-overview
CREATE TEMP FUNCTION status_order(status STRING) AS (
  -- format:off
  CASE LOWER(status)
  WHEN "draft" THEN 1
  WHEN "deleted" THEN 2
  WHEN "open" THEN 3
  WHEN "uncollectible" THEN 4
  WHEN "void" THEN 5
  WHEN "paid" THEN 6
  ELSE 0
  END
  -- format:on
);

WITH events AS (
  SELECT
    `data`.invoice.id,
    created AS event_timestamp,
    `data`.invoice.* EXCEPT (id),
    TRUE AS _from_events,
  FROM
    nonprod_events_v1
  WHERE
    `data`.invoice IS NOT NULL
    AND DATE(created) = @date
  UNION ALL
  SELECT
    *,
    FALSE AS _from_events,
  FROM
    nonprod_invoices_v1
),
ranked_events AS (
  SELECT
    * EXCEPT (_from_events),
    DENSE_RANK() OVER (
      PARTITION BY
        id
      ORDER BY
        event_timestamp DESC,
        status_order(status) DESC,
        -- when previous results would otherwise match rank with events, only
        -- aggregate the events to ensure idempotent mode-last calculation
        _from_events DESC
    ) AS _rank,
  FROM
    events
)
SELECT
  id,
  ANY_VALUE(ranked_events).* EXCEPT (id, _rank) REPLACE(
    -- collect mode-last metadata values, because there is no accurate
    -- deterministic ordering for events with the same rank
    mozfun.map.mode_last(ARRAY_CONCAT_AGG(metadata)) AS metadata
  ),
FROM
  ranked_events
WHERE
  _rank = 1
GROUP BY
  id
