-- query for org_mozilla_fenix__extract_probe_counts_v1;
CREATE TEMP FUNCTION udf_js_flatten(histogram ARRAY<STRUCT<key STRING, value FLOAT64>>)
RETURNS STRING
LANGUAGE js
AS
  '''
    let obj = {};
    histogram.map(function(r) {
        obj[r.key] = parseFloat(r.value.toFixed(4));
    });
    return JSON.stringify(obj);
''';

SELECT
  channel,
  app_version AS version,
  ping_type,
  os,
  app_build_id AS build_id,
  `moz-fx-data-shared-prod`.udf.fenix_build_to_datetime(app_build_date) AS build_date,
  metric,
  metric_type,
    -- BigQuery has some null unicode characters which Postgresql doesn't like,
    -- so we remove those here. Also limit string length to 200 to match column
    -- length.
  SUBSTR(REPLACE(key, r"\x00", ""), 0, 200) AS metric_key,
  client_agg_type,
  MAX(total_users) AS total_users,
  MAX(IF(agg_type = "histogram", udf_js_flatten(aggregates), NULL)) AS histogram,
  MAX(IF(agg_type = "percentiles", udf_js_flatten(aggregates), NULL)) AS percentiles,
FROM
  `glam_etl.org_mozilla_fenix__view_probe_counts_v1`
GROUP BY
  channel,
  app_version,
  ping_type,
  os,
  app_build_id,
  metric,
  metric_type,
  key,
  client_agg_type
