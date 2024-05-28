WITH
  base AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    * REPLACE(LOWER(client_id) AS client_id)
  FROM
    telemetry.core
  WHERE
    client_id IS NOT NULL ),
  --
  windowed AS (
  SELECT
    submission_date,
    client_id,
    ROW_NUMBER() OVER w1_unframed AS _n,
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
    udf.mode_last(ARRAY_AGG(metadata.uri.app_name) OVER w1) AS app_name,
    udf.mode_last(ARRAY_AGG(os) OVER w1) AS os,
    udf.json_mode_last(ARRAY_AGG(udf.geo_struct(metadata.geo.country, metadata.geo.city, NULL, NULL)) OVER w1).* EXCEPT (geo_subdivision1, geo_subdivision2),
    udf.mode_last(ARRAY_AGG(metadata.uri.app_build_id) OVER w1) AS app_build_id,
    udf.mode_last(ARRAY_AGG(normalized_channel) OVER w1) AS normalized_channel,
    udf.mode_last(ARRAY_AGG(locale) OVER w1) AS locale,
    udf.mode_last(ARRAY_AGG(osversion) OVER w1) AS osversion,
    udf.mode_last(ARRAY_AGG(device) OVER w1) AS device,
    udf.mode_last(ARRAY_AGG(arch) OVER w1) AS arch,
    udf.mode_last(ARRAY_AGG(default_search) OVER w1) AS default_search,
    udf.mode_last(ARRAY_AGG(distribution_id) OVER w1) AS distribution_id,
    udf.mode_last(ARRAY_AGG(campaign) OVER w1) AS campaign,
    udf.mode_last(ARRAY_AGG(campaign_id) OVER w1) AS campaign_id,
    udf.mode_last(ARRAY_AGG(default_browser) OVER w1) AS default_browser,
    udf.mode_last(ARRAY_AGG(show_tracker_stats_share) OVER w1) AS show_tracker_stats_share,
    udf.mode_last(ARRAY_AGG(metadata.uri.app_version) OVER w1) AS metadata_app_version,
    udf.mode_last(ARRAY_AGG(bug_1501329_affected) OVER w1) AS bug_1501329_affected
  FROM
    base
  WHERE
    -- Bug 1501329: avoid the pathological "canary" client_id
    client_id != 'c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0'
    -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
    AND (@submission_date IS NULL OR @submission_date = submission_date)
  WINDOW
    w1 AS (
    PARTITION BY
      client_id,
      submission_date
    ORDER BY
      submission_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    -- We must provide a modified window for ROW_NUMBER which cannot accept a frame clause.
    w1_unframed AS (
    PARTITION BY
      client_id,
      submission_date
    ORDER BY
      submission_timestamp) ),
  --
  deduped AS (
    SELECT
      * EXCEPT (_n)
    FROM
      windowed
    WHERE
      _n = 1
  )
--
SELECT
  deduped.*,
  cfs.first_seen_date,
  (cfs.first_seen_date = deduped.submission_date) AS is_new_profile,
FROM
  deduped
LEFT JOIN
  core_clients_first_seen_v1 AS cfs
  USING (client_id)
