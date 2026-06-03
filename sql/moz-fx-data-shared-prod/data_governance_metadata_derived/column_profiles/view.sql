CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.data_governance_metadata_derived.column_profiles`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.data_governance_metadata_derived.column_profiles_telemetry_derived_v1`
-- To add another profiled dataset, create a per-dataset table (e.g.
-- column_profiles_<dataset>_v1) and UNION ALL it here, for example:
--
-- UNION ALL
-- SELECT
--   *
-- FROM
--   `moz-fx-data-shared-prod.data_governance_metadata_derived.column_profiles_<dataset>_v1`
