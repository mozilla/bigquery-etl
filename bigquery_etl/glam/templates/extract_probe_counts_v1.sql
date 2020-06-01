{{ header }}

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
    app_version as version,
    ping_type,
    os,
    app_build_id as build_id,
    metric,
    metric_type,
    -- BigQuery has some null unicode characters which Postgresql doesn't like,
    -- so we remove those here. Also limit string length to 200 to match column
    -- length.
    SUBSTR(REPLACE(key, r"\x00", ""), 0, 200) AS metric_key,
    client_agg_type,
    MAX(total_users) as total_users,
    MAX(IF(agg_type = "histogram", udf_js_flatten(aggregates), NULL)) as histogram,
    MAX(IF(agg_type = "percentiles", udf_js_flatten(aggregates), NULL)) as percentiles,
FROM
    `{{ dataset }}.{{ prefix }}__view_probe_counts_v1`
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
