CREATE TEMP FUNCTION udf_js_flatten(
    histogram ARRAY<STRUCT<key STRING, value FLOAT64>>
)
RETURNS STRING
LANGUAGE js
AS
'''
    let data = histogram.map(function(r) {
        let obj = {};
        obj[r.key] = parseFloat(r.value.toFixed(4));
        return obj;
    });
    return JSON.stringify(data);
''';

SELECT
    app_version,
    COALESCE(os, "*") AS os,
    COALESCE(app_build_id, "*") AS app_build_id,
    process,
    metric,
    -- BigQuery has some null unicode characters which Postgresql doesn't like, so we remove those here.
    -- Also limit string length to 200 to match column length.
    SUBSTR(REPLACE(key, r"\x00", ""), 0, 200) AS key,
    client_agg_type,
    metric_type,
    MAX(total_users) AS total_users,
    -- Using MAX instead of COALESCE since this is not in the GROUP BY.
    MAX(IF(agg_type="histogram", udf_js_flatten(aggregates), NULL)) AS histogram,
    MAX(IF(agg_type="percentiles", udf_js_flatten(aggregates), NULL)) AS percentiles
FROM
    `moz-fx-data-shared-prod.telemetry.client_probe_counts`
WHERE
    channel = @channel
    AND app_version IS NOT NULL
    AND total_users > 1000
GROUP BY
    channel, app_version, app_build_id, os, metric, metric_type, key,
    process, client_agg_type
