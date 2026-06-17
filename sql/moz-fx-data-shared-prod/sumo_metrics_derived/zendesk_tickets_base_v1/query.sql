WITH tickets AS (
  SELECT
    DATE(TIMESTAMP(t.created_at), "UTC") AS date_utc,
    -- Map product names to match SSSR conventions
    COALESCE(m.product_mapping, t.custom_product) AS product,
    t.id AS ticket_id
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket` t
  LEFT JOIN
    `moz-fx-data-shared-prod.static.cx_product_mappings_v1` m
    ON m.product = t.custom_product
    AND m.source = 'Zendesk'
  WHERE
    DATE(TIMESTAMP(t.created_at), "UTC") = @submission_date
)
SELECT
  date_utc AS `date`,
  product,
  COUNT(DISTINCT ticket_id) AS zendesk_tickets_created,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  tickets
GROUP BY
  `date`,
  product
