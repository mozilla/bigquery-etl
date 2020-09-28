-- This is an authorized view that allows us to read specific data from
-- a project owned by the Pocket team; if the definition here changes,
-- it may need to be manually redeployed by Data Ops.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pocket.pocket_reach_mau`
AS
SELECT
  *
FROM
  `pocket-airflow-prod.data_from_pocket.pocket_reach_mau`
