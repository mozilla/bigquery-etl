{{ header }}

SELECT
    channel,
    app_version as version,
    ping_type,
    os,
    app_build_id as build_id,
    IF(app_build_id="*", NULL, SAFE_CAST({{ build_date_udf }}(app_build_id) AS STRING)) as build_date,
    metric,
    metric_type,
    -- BigQuery has some null unicode characters which Postgresql doesn't like,
    -- so we remove those here. Also limit string length to 200 to match column
    -- length.
    SUBSTR(REPLACE(key, r"\x00", ""), 0, 200) AS metric_key,
    client_agg_type,
    MAX(total_users) as total_users,
    MAX(IF(agg_type = "histogram", mozfun.glam.histogram_cast_json(aggregates), NULL)) as histogram,
    MAX(IF(agg_type = "percentiles", mozfun.glam.histogram_cast_json(aggregates), NULL)) as percentiles,
FROM
    `{{ dataset }}.{{ prefix }}__view_probe_counts_v1`
WHERE
    total_users > 10
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
