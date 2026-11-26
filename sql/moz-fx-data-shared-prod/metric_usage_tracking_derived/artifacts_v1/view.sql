-- Every source's artifacts table. FULL UNION ALL BY NAME matches columns by name, so a
-- column added to the job's schema is null for a source whose table it is not
-- in yet. A new source in the job (common.SOURCES) has to be added here.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.artifacts_v1`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.artifacts_bqetl_v1`
FULL UNION ALL BY NAME
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.artifacts_redash_v1`
FULL UNION ALL BY NAME
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.artifacts_looker_v1`
FULL UNION ALL BY NAME
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.artifacts_glam_v1`
FULL UNION ALL BY NAME
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.artifacts_metrichub_v1`
FULL UNION ALL BY NAME
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.artifacts_adhoc_v1`
