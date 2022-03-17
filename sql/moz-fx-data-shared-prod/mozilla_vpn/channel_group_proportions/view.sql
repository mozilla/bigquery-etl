CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.channel_group_proportions`
AS
WITH max_agg_date AS (
  SELECT AS VALUE
    MAX(subscription_start_date)
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn_derived.channel_group_proportions_v1
)
SELECT
  mozilla_vpn_derived.channel_group_proportions_live.*
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.channel_group_proportions_live
CROSS JOIN
  max_agg_date
WHERE
  subscription_start_date > max_agg_date
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.channel_group_proportions_v1
