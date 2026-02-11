WITH unnested_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    events.category AS event_category,
    events.name AS event_name,
    normalized_channel,
    app_version_major,
    normalized_os,
    normalized_os_version,
    client_info.windows_build_number,
    events
  FROM
    `mozdata.firefox_desktop.profile_restore` AS restored_profile,
    UNNEST(restored_profile.events) AS events
  WHERE
    DATE(submission_timestamp) = @submission_date
)
SELECT
  submission_date,
  client_id,
  event_category,
  event_name,
  normalized_channel,
  app_version_major,
  normalized_os,
  normalized_os_version,
  windows_build_number,
  REGEXP_REPLACE(extra.value, r'[{}]', '') AS restore_id
FROM
  unnested_events,
  UNNEST(events.extra) AS extra
WHERE
  extra.key = "restore_id"
