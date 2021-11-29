-- query for firefox_desktop_glam_nightly__histogram_percentiles_v1;
SELECT
  * EXCEPT (aggregates) REPLACE('percentiles' AS agg_type),
  ARRAY<STRUCT<key STRING, value FLOAT64>>[
    ('5', mozfun.glam.percentile(5, aggregates, metric_type)),
    ('25', mozfun.glam.percentile(25, aggregates, metric_type)),
    ('50', mozfun.glam.percentile(50, aggregates, metric_type)),
    ('75', mozfun.glam.percentile(75, aggregates, metric_type)),
    ('95', mozfun.glam.percentile(95, aggregates, metric_type)),
    ('99', mozfun.glam.percentile(99, aggregates, metric_type)),
    ('99.9', mozfun.glam.percentile(99.9, aggregates, metric_type))
  ] AS aggregates
FROM
  glam_etl.firefox_desktop_glam_nightly__histogram_probe_counts_v1
