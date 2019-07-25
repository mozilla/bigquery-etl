WITH total AS (
SELECT
    SUM(histogram_parent_http_pageload_is_ssl[SAFE_OFFSET(0)].value + histogram_parent_http_pageload_is_ssl[SAFE_OFFSET(1)].value) AS loads
FROM main_summary_v4
WHERE
    sample_id = 42
    AND submission_date = @submission_date
    AND normalized_channel = 'release'
    AND os IN ('Windows_NT', 'Darwin', 'Linux')
    AND app_name = 'Firefox'
)
SELECT
    submission_date,
    os,
    country,
    1.0 * COUNT(histogram_parent_http_pageload_is_ssl) / COUNT(submission_date_s3)  AS reporting_ratio, -- ratio of pings that have the probe
    1.0 * SUM(histogram_parent_http_pageload_is_ssl[SAFE_OFFSET(0)].value + histogram_parent_http_pageload_is_ssl[SAFE_OFFSET(1)].value)  / udf_mode_last(array_agg(total.loads)) AS normalized_pageloads, -- normalized count of pageloads that went into this ratio
    1.0 * SUM(histogram_parent_http_pageload_is_ssl[SAFE_OFFSET(1)].value)  / SUM(histogram_parent_http_pageload_is_ssl[SAFE_OFFSET(0)].value + histogram_parent_http_pageload_is_ssl[SAFE_OFFSET(1)].value) AS ratio
FROM main_summary_v4, total
WHERE
    sample_id = 42
    AND submission_date = @submission_date
    AND normalized_channel = 'release'
    AND os IN ('Windows_NT', 'Darwin', 'Linux')
    AND app_name = 'Firefox'
GROUP BY 1, 2, 3--, 4, 5, 6
HAVING SUM(histogram_parent_http_pageload_is_ssl[SAFE_OFFSET(0)] + histogram_parent_http_pageload_is_ssl[SAFE_OFFSET(1)]) > 5000 -- you have to have loaded some amount.
