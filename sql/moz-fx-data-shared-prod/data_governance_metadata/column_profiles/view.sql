CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.data_governance_metadata.column_profiles`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.data_governance_metadata_derived.column_profiles_v1`
-- Unversioned read surface over the single column_profiles_v1 table. Additional
-- source datasets are added via the job's --source-datasets argument (they land
-- in the same table tagged by source_dataset), so no view change is needed unless
-- there is a version bump.
