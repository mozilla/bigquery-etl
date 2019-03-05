WITH
  windowed AS (
  SELECT
    @submission_date AS submission_date,
    CURRENT_DATETIME() AS generated_time,
    client_id,
    ROW_NUMBER() OVER w1 AS n,
    -- For now, we're ignoring the following RECORD type fields:
    --   accessibility_services
    --   experiments
    --   searches
    --
    -- Take the min observed profile creation date.
    DATE_FROM_UNIX_DATE(MIN(profile_date) OVER w1) AS profile_date,
    -- These integer fields are already sums over sessions since last upload,
    -- so we sum to represent all uploads in the given day.
    SUM(durations) OVER w1 AS durations,
    SUM(flash_usage) OVER w1 AS flash_usage,
    -- For all other fields, we take the value in the most recently received ping.
    LAST_VALUE(app_name) OVER w1 AS app_name,
    LAST_VALUE(os) OVER w1 AS os,
    LAST_VALUE(metadata.geo_country) OVER w1 AS country,
    LAST_VALUE(metadata.geo_city) OVER w1 AS city,
    LAST_VALUE(metadata.app_build_id) OVER w1 AS app_build_id,
    LAST_VALUE(metadata.normalized_channel) OVER w1 AS normalized_channel,
    LAST_VALUE(locale) OVER w1 AS locale,
    LAST_VALUE(osversion) OVER w1 AS osversion,
    LAST_VALUE(device) OVER w1 AS device,
    LAST_VALUE(arch) OVER w1 AS arch,
    LAST_VALUE(default_search) OVER w1 AS default_search,
    LAST_VALUE(distribution_id) OVER w1 AS distribution_id,
    LAST_VALUE(campaign) OVER w1 AS campaign,
    LAST_VALUE(campaign_id) OVER w1 AS campaign_id,
    LAST_VALUE(default_browser) OVER w1 AS default_browser,
    LAST_VALUE(show_tracker_stats_share) OVER w1 AS show_tracker_stats_share,
    LAST_VALUE(metadata_app_version) OVER w1 AS metadata_app_version,
    LAST_VALUE(bug_1501329_affected) OVER w1 AS bug_1501329_affected
  FROM
    telemetry_core_parquet_v3
  WHERE
    submission_date_s3 = @submission_date
  WINDOW
    w1 AS (
    PARTITION BY
      client_id,
      submission_date_s3
    ORDER BY
      submission_date_s3) )
SELECT
  * EXCEPT (n)
FROM
  windowed
WHERE
  n = 1
