-- This is an authorized view that allows us to read specific data from
-- a project owned by CloudOps; if the definition here changes,
-- it may need to be manually redeployed by Data Ops.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.buildhub2`
AS
SELECT
  *
FROM
  `moz-fx-buildhub2-prod-4784.build_metadata.buildhub2`
