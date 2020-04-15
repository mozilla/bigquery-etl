SELECT
  * EXCEPT (aggregates) REPLACE('percentiles' AS agg_type),
  ARRAY<STRUCT<key STRING, value FLOAT64>>[
    ('5', udf_js.glean_percentile(5, aggregates, metric_type)),
    ('25', udf_js.glean_percentile(25, aggregates, metric_type)),
    ('50', udf_js.glean_percentile(50, aggregates, metric_type)),
    ('75', udf_js.glean_percentile(75, aggregates, metric_type)),
    ('95', udf_js.glean_percentile(95, aggregates, metric_type))
  ] AS aggregates
FROM
  glam_etl.org_mozilla_fenix__clients_histogram_probe_counts_v1
