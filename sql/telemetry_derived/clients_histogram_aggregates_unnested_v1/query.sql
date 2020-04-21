SELECT
    sample_id,
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    aggregates,
    os = 'Windows'and channel = 'release' AS sampled
FROM
    clients_histogram_aggregates_v1
CROSS JOIN UNNEST(histogram_aggregates)
WHERE submission_date = @submission_date
    AND first_bucket IS NOT NULL