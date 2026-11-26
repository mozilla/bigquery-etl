-- Every source's refs table. FULL UNION ALL BY NAME matches columns by name, so a
-- column added to the job's schema is null for a source whose table it is not
-- in yet. A new source in the job (common.SOURCES) has to be added here.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.refs_v1`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.refs_bqetl_v1`
FULL UNION ALL BY NAME
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.refs_redash_v1`
FULL UNION ALL BY NAME
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.refs_looker_v1`
FULL UNION ALL BY NAME
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.refs_glam_v1`
FULL UNION ALL BY NAME
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.refs_metrichub_v1`
FULL UNION ALL BY NAME
SELECT
  *
FROM
  `moz-fx-data-shared-prod.metric_usage_tracking_derived.refs_adhoc_v1`
