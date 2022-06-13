{{ header }}
SELECT
  * EXCEPT (aggregates) REPLACE('percentiles' AS agg_type),
  ARRAY<STRUCT<key STRING, value FLOAT64>>[
    ('0.1', mozfun.glam.percentile(0.1, aggregates, metric_type)),
    ('1', mozfun.glam.percentile(1, aggregates, metric_type)),
    ('5', mozfun.glam.percentile(5, aggregates, metric_type)),
    ('25', mozfun.glam.percentile(25, aggregates, metric_type)),
    ('50', mozfun.glam.percentile(50, aggregates, metric_type)),
    ('75', mozfun.glam.percentile(75, aggregates, metric_type)),
    ('95', mozfun.glam.percentile(95, aggregates, metric_type)),
    ('99', mozfun.glam.percentile(99, aggregates, metric_type)),
    ('99.9', mozfun.glam.percentile(99.9, aggregates, metric_type))
  ] AS aggregates
FROM
  glam_etl.{{ prefix }}__histogram_probe_counts_v1
