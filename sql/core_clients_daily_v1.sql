CREATE TEMP FUNCTION udf_mode_last(x ANY TYPE) AS ((
  SELECT
    val
  FROM (
    SELECT
      val,
      COUNT(val) AS n,
      MAX(offset) AS max_offset
    FROM
      UNNEST(x) AS val
    WITH OFFSET AS offset
    GROUP BY
      val
    ORDER BY
      n DESC,
      max_offset DESC
  )
  LIMIT 1
));

WITH
  numbered_duplicates AS (
  SELECT
    * REPLACE(LOWER(client_id) AS client_id),
    ROW_NUMBER() OVER (PARTITION BY client_id, submission_date_s3, metadata.document_id ORDER BY metadata.timestamp DESC) AS n
  FROM
    telemetry_core_parquet_v3
  WHERE
    client_id IS NOT NULL ),
  -- Deduplicating on document_id is necessary to get valid SUM values.
  deduplicated AS (
  SELECT
    * EXCEPT (n)
  FROM
    numbered_duplicates
  WHERE
    n = 1 ),
  windowed AS (
  SELECT
    @submission_date AS submission_date,
    CURRENT_DATETIME() AS generated_time,
    client_id,
    ROW_NUMBER() OVER w1_unframed AS n,
    -- For now, we're ignoring the following RECORD type fields:
    --   accessibility_services
    --   experiments
    --   searches
    --
    -- Take the min observed profile creation date.
    SAFE.DATE_FROM_UNIX_DATE(MIN(profile_date) OVER w1) AS profile_date,
    -- These integer fields are already sums over sessions since last upload,
    -- so we sum to represent all uploads in the given day;
    -- we set an upper limit of 100K which contains 99.9th percentile of durations
    -- while avoiding integer overflow on pathological input.
    SUM(IF(sessions BETWEEN 0 AND 100000, sessions, 0)) OVER w1 AS sessions,
    SUM(IF(durations BETWEEN 0 AND 100000, durations, 0)) OVER w1 AS durations,
    SUM(IF(flash_usage BETWEEN 0 AND 100000, flash_usage, 0)) OVER w1 AS flash_usage,
    -- For all other dimensions, we use the mode of observed values in the day.
    udf_mode_last(ARRAY_AGG(app_name) OVER w1) AS app_name,
    udf_mode_last(ARRAY_AGG(os) OVER w1) AS os,
    udf_mode_last(ARRAY_AGG(metadata.geo_country) OVER w1) AS country,
    udf_mode_last(ARRAY_AGG(metadata.geo_city) OVER w1) AS city,
    udf_mode_last(ARRAY_AGG(metadata.app_build_id) OVER w1) AS app_build_id,
    udf_mode_last(ARRAY_AGG(metadata.normalized_channel) OVER w1) AS normalized_channel,
    udf_mode_last(ARRAY_AGG(locale) OVER w1) AS locale,
    udf_mode_last(ARRAY_AGG(osversion) OVER w1) AS osversion,
    udf_mode_last(ARRAY_AGG(device) OVER w1) AS device,
    udf_mode_last(ARRAY_AGG(arch) OVER w1) AS arch,
    udf_mode_last(ARRAY_AGG(default_search) OVER w1) AS default_search,
    udf_mode_last(ARRAY_AGG(distribution_id) OVER w1) AS distribution_id,
    udf_mode_last(ARRAY_AGG(campaign) OVER w1) AS campaign,
    udf_mode_last(ARRAY_AGG(campaign_id) OVER w1) AS campaign_id,
    udf_mode_last(ARRAY_AGG(default_browser) OVER w1) AS default_browser,
    udf_mode_last(ARRAY_AGG(show_tracker_stats_share) OVER w1) AS show_tracker_stats_share,
    udf_mode_last(ARRAY_AGG(metadata_app_version) OVER w1) AS metadata_app_version,
    udf_mode_last(ARRAY_AGG(bug_1501329_affected) OVER w1) AS bug_1501329_affected
  FROM
    deduplicated
  WHERE
    submission_date_s3 = @submission_date
    -- Bug 1501329: avoid the pathological "canary" client_id
    AND client_id != 'c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0'
  WINDOW
    w1 AS (
    PARTITION BY
      client_id,
      submission_date_s3
    ORDER BY
      metadata.timestamp DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    -- We must provide a modified window for ROW_NUMBER which cannot accept a frame clause.
    w1_unframed AS (
    PARTITION BY
      client_id,
      submission_date_s3
    ORDER BY
      metadata.timestamp DESC) )
SELECT
  * EXCEPT (n)
FROM
  windowed
WHERE
  n = 1
