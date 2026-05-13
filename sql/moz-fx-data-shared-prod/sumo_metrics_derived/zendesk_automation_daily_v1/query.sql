-- Classify tickets as 'Appbot - Non-English' to exclude auto-closed non-English app review tickets.
-- See zen_desk_volume.sql for context: these are auto-closed under a fake account (SUMOJr)
-- and are never seen by agents.
WITH appbot_class AS (
  SELECT
    ticket_id,
    CASE
      WHEN LOGICAL_OR(tag = 'appbot')
        AND LOGICAL_OR(tag IN ('english', 'usa', 'unitedkingdom', 'canada', 'australia'))
        THEN 'Appbot - English'
      WHEN LOGICAL_OR(tag = 'appbot')
        AND NOT LOGICAL_OR(tag IN ('english', 'usa', 'unitedkingdom', 'canada', 'australia'))
        THEN 'Appbot - Non-English'
      ELSE 'All Other Tickets'
    END AS ticket_group
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
  GROUP BY
    ticket_id
),
reopens AS (
  SELECT
    ticket_id,
    COUNT(*) AS reopen_count
  FROM
    (
      SELECT
        ticket_id,
        LAG(ticket_id, 1, 0) OVER (PARTITION BY ticket_id ORDER BY updated) AS prev_ticket_id,
        value AS status,
        LAG(value, 1, 'new') OVER (PARTITION BY ticket_id ORDER BY updated) AS prev_status
      FROM
        `moz-fx-data-shared-prod.zendesk_syndicate.ticket_field_history`
      WHERE
        field_name = 'status'
    )
  WHERE
    ticket_id = prev_ticket_id
    AND prev_status = 'solved'
    AND status = 'open'
  GROUP BY
    ticket_id
),
automation_tags AS (
  SELECT
    ticket_id,
    MAX(
      CASE
        WHEN tag IN (
            'ssa-sign-in-failure-automation',
            'ssa-connection-issues-automation',
            'ssa-sync-data-automation',
            'appbot-autosolve',
            'ssa-experiment-2fa-automation',
            'ssa-experiment-pwrdreset-automation',
            'ssa-experiment-emailverify-automation',
            'ssa-experiment-4-star',
            'ssa-experiment-5-star',
            'loginless-autosolve'
          )
          THEN 1
        ELSE 0
      END
    ) AS is_automated
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
  GROUP BY
    ticket_id
),
automation_classification AS (
  SELECT
    aut.ticket_id,
    CASE
      WHEN aut.is_automated = 1
        AND COALESCE(r.reopen_count, 0) > 0
        THEN 'human-handled'
      WHEN aut.is_automated = 1
        THEN 'automation'
      ELSE 'human-handled'
    END AS automation_category
  FROM
    automation_tags aut
  LEFT JOIN
    reopens r
    ON aut.ticket_id = r.ticket_id
),
solve_dates AS (
  SELECT
    ticket_id,
    MAX(CASE WHEN value = 'solved' THEN updated END) AS solved_at,
    MAX(CASE WHEN value = 'closed' THEN updated END) AS closed_at
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket_field_history`
  WHERE
    field_name = 'status'
  GROUP BY
    ticket_id
),
test_tickets AS (
  SELECT DISTINCT
    ticket_id
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
  WHERE
    tag LIKE '%test%'
),
tickets AS (
  SELECT
    DATE(TIMESTAMP(t.created_at), "UTC") AS created_date,
    DATE(COALESCE(sd.solved_at, sd.closed_at)) AS resolved_date,
    COALESCE(m.product_mapping, t.custom_product) AS product,
    COALESCE(ac.automation_category, 'human-handled') AS automation_category,
    t.id AS ticket_id
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket` t
  LEFT JOIN
    appbot_class ab
    ON t.id = ab.ticket_id
  LEFT JOIN
    `moz-fx-data-shared-prod.static.cx_product_mappings_v1` m
    ON m.product = t.custom_product
    AND m.source = 'Zendesk'
  LEFT JOIN
    automation_classification ac
    ON t.id = ac.ticket_id
  LEFT JOIN
    solve_dates sd
    ON t.id = sd.ticket_id
  LEFT JOIN
    test_tickets tt
    ON t.id = tt.ticket_id
  LEFT JOIN
    `moz-fx-data-shared-prod.zendesk_syndicate.group` g
    ON t.group_id = g.id
  WHERE
    COALESCE(ab.ticket_group, 'All Other Tickets') != 'Appbot - Non-English'
    AND t.status != 'deleted'
    AND tt.ticket_id IS NULL
    AND COALESCE(g.name, '') NOT IN ('Sumo Test', 'VPN QA')
),
events AS (
  SELECT
    created_date AS `date`,
    product,
    automation_category,
    'created' AS event_type
  FROM
    tickets
  WHERE
    created_date IS NOT NULL
  UNION ALL
  SELECT
    resolved_date AS `date`,
    product,
    automation_category,
    'resolved' AS event_type
  FROM
    tickets
  WHERE
    resolved_date IS NOT NULL
)
SELECT
  `date`,
  product,
  automation_category,
  COUNTIF(event_type = 'created') AS tickets_created,
  COUNTIF(event_type = 'resolved') AS tickets_resolved,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  events
GROUP BY
  `date`,
  product,
  automation_category
ORDER BY
  `date`,
  product,
  automation_category
