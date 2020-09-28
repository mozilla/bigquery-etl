-- view for org_mozilla_fenix_glam_release__view_probe_counts_v1;
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glam_etl.org_mozilla_fenix_glam_release__view_probe_counts_v1`
AS
WITH all_counts AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.glam_etl.org_mozilla_fenix_glam_release__scalar_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.glam_etl.org_mozilla_fenix_glam_release__histogram_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.glam_etl.org_mozilla_fenix_glam_release__scalar_percentiles_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.glam_etl.org_mozilla_fenix_glam_release__histogram_percentiles_v1`
)
SELECT
  *
FROM
  all_counts
