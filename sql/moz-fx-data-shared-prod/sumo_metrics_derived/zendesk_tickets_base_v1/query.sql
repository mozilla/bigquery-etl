WITH tickets AS (
  SELECT
    DATE(TIMESTAMP(t.created_at), "UTC") AS date_utc,
    -- Map product names to match SSSR conventions
    mozfun.customer_experience.normalize_product(t.custom_product, 'Zendesk') AS product,
    t.id AS ticket_id
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket` t
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
