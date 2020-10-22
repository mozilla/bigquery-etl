{{ header }}
CREATE OR REPLACE VIEW
  `{{ project }}.{{ dataset }}.{{ prefix }}__view_probe_counts_v1`
AS
WITH all_counts AS (
  SELECT
    *
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__scalar_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__histogram_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__scalar_percentiles_v1`
  UNION ALL
  SELECT
    *
  FROM
    `{{ project }}.{{ dataset }}.{{ prefix }}__histogram_percentiles_v1`
)
SELECT
  *
FROM
  all_counts
