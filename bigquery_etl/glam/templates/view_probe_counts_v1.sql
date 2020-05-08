CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}_view_probe_counts_v1`
AS
WITH all_counts AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}_clients_scalar_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}_clients_histogram_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}_scalar_percentiles_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}_histogram_percentiles_v1`
)
SELECT
  *
FROM
  all_counts
