WITH tickets AS (
  SELECT
    DATE(TIMESTAMP(created_at), "UTC") AS date_utc,
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
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket`
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
