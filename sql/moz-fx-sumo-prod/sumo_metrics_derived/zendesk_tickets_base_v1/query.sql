WITH tickets AS (
  SELECT
    DATE(TIMESTAMP(created_at), "America/Los_Angeles") AS date_pst,
      -- Map product names to match SSSR conventions
    CASE
      custom_product
      WHEN 'firefox-android'
        THEN 'firefox-android-reviews'
      WHEN 'firefox-ios'
        THEN 'firefox-ios-reviews'
      WHEN 'firefox-private-network-vpn'
        THEN 'mozilla-vpn'
      WHEN 'vpn_relay_bundle'
        THEN 'mozilla-vpn'
      ELSE custom_product
    END AS product,
    id AS ticket_id
  FROM
    `moz-fx-sumo-prod.zendesk.ticket`
  WHERE
    DATE(TIMESTAMP(created_at), "America/Los_Angeles")
    BETWEEN '2024-07-01'
    AND '2026-01-05'
)
SELECT
  date_pst AS `date`,
  product,
  COUNT(DISTINCT ticket_id) AS zendesk_tickets_created,
  EXTRACT(YEAR FROM date_pst) AS year,
  EXTRACT(MONTH FROM date_pst) AS month,
  EXTRACT(WEEK FROM date_pst) AS week_number,
  EXTRACT(DAYOFWEEK FROM date_pst) AS day_of_week,
  FORMAT_DATE('%Y-%m', date_pst) AS year_month,
  FORMAT_DATE('%Y-W%W', date_pst) AS year_week,
  FORMAT_DATE('%A', date_pst) AS day_name,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  tickets
GROUP BY
  date,
  product
