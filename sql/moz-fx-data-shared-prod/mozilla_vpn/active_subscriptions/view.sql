CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.active_subscriptions`
AS
WITH max_agg_date AS (
  SELECT AS VALUE
    MAX(active_date)
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn_derived.active_subscriptions_v1
)
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.active_subscriptions
CROSS JOIN
  max_agg_date
WHERE
  active_date > max_agg_date
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.active_subscriptions_v1
