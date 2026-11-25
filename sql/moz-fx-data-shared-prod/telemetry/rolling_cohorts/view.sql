CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.rolling_cohorts`
AS
SELECT
  -- is_automated is NULL in cohorts written before DENG-12035 that were not backfilled. Those
  -- are older than every confirmed bot fleet, so return FALSE: then NOT is_automated keeps them.
  * REPLACE (COALESCE(is_automated, FALSE) AS is_automated)
FROM
  `moz-fx-data-shared-prod.telemetry_derived.rolling_cohorts_v2`
