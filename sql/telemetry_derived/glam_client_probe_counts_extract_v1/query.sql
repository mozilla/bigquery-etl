SELECT
    CASE
        WHEN channel="nightly" THEN 1
        WHEN channel="beta" THEN 2
        WHEN channel="release" THEN 3
    END AS channel,
    app_version,
    CASE
        WHEN agg_type="histogram" THEN 1
        WHEN agg_type="percentiles" THEN 2
    END AS agg_type,
    COALESCE(os, "*") AS os,
    COALESCE(app_build_id, "*") AS app_build_id,
    CASE
        WHEN process IS NULL THEN 0
        WHEN process="parent" THEN 1
        WHEN process="content" THEN 2
        WHEN process="gpu" THEN 3
    END AS process,
    metric,
    -- BigQuery has some null unicode characters which Postgresql doesn't like, so we remove those here.
    -- Also limit string length to 200 to match column length.
    SUBSTR(REPLACE(key, r"\x00", ""), 0, 200) AS key,
    client_agg_type,
    metric_type,
    total_users,
    TO_JSON_STRING(aggregates) AS aggregates
FROM
    `moz-fx-data-shared-prod.telemetry.client_probe_counts`
WHERE
    channel = @channel
    AND app_version IS NOT NULL
    AND total_users > 1000
