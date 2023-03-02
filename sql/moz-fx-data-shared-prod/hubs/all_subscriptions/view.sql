CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.hubs.all_subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.hubs_derived.all_subscriptions_v1
