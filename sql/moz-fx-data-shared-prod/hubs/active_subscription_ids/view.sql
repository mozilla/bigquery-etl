CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.hubs.active_subscription_ids`
AS
WITH max_active_date AS (
  SELECT AS VALUE
    MAX(active_date)
  FROM
    `moz-fx-data-shared-prod`.hubs_derived.active_subscription_ids_v1
)
SELECT
  active_subscription_ids_live.*
FROM
  `moz-fx-data-shared-prod`.hubs_derived.active_subscription_ids_live
CROSS JOIN
  max_active_date
WHERE
  -- static partition filter not needed because live view doesn't use date partitioned tables
  active_date > max_active_date
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.hubs_derived.active_subscription_ids_v1
