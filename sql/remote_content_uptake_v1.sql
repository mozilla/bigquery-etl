CREATE TEMP FUNCTION
  udf_get_key_from_map_entry(entry ANY TYPE,
    key ANY TYPE) AS (
  IF
    (entry.key = key,
      entry.value,
      NULL));
  --
SELECT
  submission_date,
  uptake.key AS `source`,
  -- These timestamps help us display a date range when we present the
  -- data. It helps to "remind" you when something first started appearing.
  MIN(`timestamp`/1000000000) AS min_timestamp,
  MAX(`timestamp`/1000000000) AS max_timestamp,
  -- Create a column for each label on UPTAKE_REMOTE_CONTENT_RESULT_1 in
  -- https://hg.mozilla.org/mozilla-central/file/tip/toolkit/components/telemetry/Histograms.json
  SUM(udf_get_key_from_map_entry(status,
      'up_to_date')) AS up_to_date,
  SUM(udf_get_key_from_map_entry(status,
      'success')) AS success,
  SUM(udf_get_key_from_map_entry(status,
      'backoff')) AS backoff,
  SUM(udf_get_key_from_map_entry(status,
      'pref_disabled')) AS pref_disabled,
  SUM(udf_get_key_from_map_entry(status,
      'parse_error')) AS parse_error,
  SUM(udf_get_key_from_map_entry(status,
      'content_error')) AS content_error,
  SUM(udf_get_key_from_map_entry(status,
      'sign_error')) AS sign_error,
  SUM(udf_get_key_from_map_entry(status,
      'sign_retry_error')) AS sign_retry_error,
  SUM(udf_get_key_from_map_entry(status,
      'conflict_error')) AS conflict_error,
  SUM(udf_get_key_from_map_entry(status,
      'sync_error')) AS sync_error,
  SUM(udf_get_key_from_map_entry(status,
      'apply_error')) AS apply_error,
  SUM(udf_get_key_from_map_entry(status,
      'server_error')) AS server_error,
  SUM(udf_get_key_from_map_entry(status,
      'certificate_error')) AS certificate_error,
  SUM(udf_get_key_from_map_entry(status,
      'download_error')) AS download_error,
  SUM(udf_get_key_from_map_entry(status,
      'timeout_error')) AS timeout_error,
  SUM(udf_get_key_from_map_entry(status,
      'network_error')) AS network_error,
  SUM(udf_get_key_from_map_entry(status,
      'offline_error')) AS offline_error,
  SUM(udf_get_key_from_map_entry(status,
      'cleanup_error')) AS cleanup_error,
  SUM(udf_get_key_from_map_entry(status,
      'unknown_error')) AS unknown_error,
  SUM(udf_get_key_from_map_entry(status,
      'custom_1_error')) AS custom_1_error,
  SUM(udf_get_key_from_map_entry(status,
      'custom_2_error')) AS custom_2_error,
  SUM(udf_get_key_from_map_entry(status,
      'custom_3_error')) AS custom_3_error,
  SUM(udf_get_key_from_map_entry(status,
      'custom_4_error')) AS custom_4_error,
  SUM(udf_get_key_from_map_entry(status,
      'custom_5_error')) AS custom_5_error
FROM
  main_summary,
  UNNEST(histogram_parent_uptake_remote_content_result_1.key_value) AS uptake,
  UNNEST(uptake.value.key_value) AS status
WHERE
  submission_date_s3 = @submission_date
  AND sample_id = 42
GROUP BY
  submission_date_s3,
  `source`
ORDER BY
  `source`
