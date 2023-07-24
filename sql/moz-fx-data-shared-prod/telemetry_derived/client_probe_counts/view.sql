CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.telemetry_derived.client_probe_counts AS (
  WITH
    all_counts AS (
    SELECT
      *,
      aggregates AS non_norm_aggregates
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.clients_scalar_probe_counts_v1`
    UNION ALL
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.clients_histogram_probe_counts_v1`
    UNION ALL
    SELECT
      *,
      aggregates AS non_norm_aggregates
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.scalar_percentiles_v1`
    UNION ALL
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.histogram_percentiles_v1` )
  SELECT
    *
  FROM
    all_counts)
