CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}__view_probe_counts_v1`
AS
WITH all_counts AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}__clients_scalar_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}__clients_histogram_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}__scalar_percentiles_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}__histogram_percentiles_v1`
)
SELECT
  *
FROM
  all_counts
