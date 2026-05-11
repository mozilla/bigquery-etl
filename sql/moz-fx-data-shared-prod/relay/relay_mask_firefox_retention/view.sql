CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.relay.relay_mask_firefox_retention`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.relay_derived.relay_mask_firefox_retention_v1
