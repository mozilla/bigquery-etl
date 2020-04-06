CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glam_etl.fenix_view_client_probe_counts_v1`
AS
WITH all_counts AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.glam_etl.fenix_clients_scalar_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.glam_etl.fenix_clients_histogram_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.glam_etl.fenix_scalar_percentiles_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.glam_etl.fenix_histogram_percentiles_v1`
)
SELECT
  *
FROM
  all_counts
