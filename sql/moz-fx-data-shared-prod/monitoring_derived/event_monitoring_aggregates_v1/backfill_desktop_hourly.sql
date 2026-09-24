-- Hourly chunk of the firefox_desktop portion of event_monitoring_aggregates_v1.
-- Produces the same output as firefox_desktop_derived.event_monitoring_live_v1,
-- restricted to one hour of live table data so each run stays small. Live tables
-- are clustered on submission_timestamp, so each run only scans about one hour of data.
--
-- The MV crosses every event with every extra and every experiment, which is
-- expensive because desktop pings carry up to ~50 experiments. This version first
-- collapses each ping's events into (category, name, extra key) counts, then
-- aggregates across pings sharing the same experiment set (keyed by a fingerprint),
-- and only expands experiments at the end. This used about 11x fewer slot-ms than
-- the MV logic for 2026-09-22 00:00 with identical output (MVs don't support this type of subquery).
-- Output rows for different hours never collide (window_start is in the grain),
-- so the runs can be appended to one staging table. Live tables keep 30 days.
--
-- Run once per hour of each day being backfilled, for example
-- (backfill-4 instead of 3 is intentional because it's less busy):
--   for d in 2026-09-22 2026-09-23; do for h in $(seq -w 0 23); do
--     bq --project_id=moz-fx-data-backfill-4 query --use_legacy_sql=false --max_rows=0 \
--       --parameter="window_start:TIMESTAMP:$d $h:00:00" \
--       --destination_table=moz-fx-data-shared-prod:tmp.event_monitoring_desktop_hourly \
--       --append_table \
--       < sql/moz-fx-data-shared-prod/monitoring_derived/event_monitoring_aggregates_v1/backfill_desktop_hourly.sql \
--       || { echo "failed at $d $h"; break 2; }
--   done; done
-- The job runs in (and is billed to) the --project_id project.
WITH pings AS (
  SELECT
    'data_leak_blocker_v1' AS document_table,
    submission_timestamp,
    normalized_channel,
    sample_id,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
    ping_info.experiments,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.data_leak_blocker_v1`
  WHERE
    submission_timestamp >= @window_start
    AND submission_timestamp < TIMESTAMP_ADD(@window_start, INTERVAL 1 HOUR)
  UNION ALL
  SELECT
    'events_v1' AS document_table,
    submission_timestamp,
    normalized_channel,
    sample_id,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
    ping_info.experiments,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.events_v1`
  WHERE
    submission_timestamp >= @window_start
    AND submission_timestamp < TIMESTAMP_ADD(@window_start, INTERVAL 1 HOUR)
  UNION ALL
  SELECT
    'newtab_v1' AS document_table,
    submission_timestamp,
    normalized_channel,
    sample_id,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
    ping_info.experiments,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
  WHERE
    submission_timestamp >= @window_start
    AND submission_timestamp < TIMESTAMP_ADD(@window_start, INTERVAL 1 HOUR)
  UNION ALL
  SELECT
    'nimbus_targeting_context_v1' AS document_table,
    submission_timestamp,
    normalized_channel,
    sample_id,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
    ping_info.experiments,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.nimbus_targeting_context_v1`
  WHERE
    submission_timestamp >= @window_start
    AND submission_timestamp < TIMESTAMP_ADD(@window_start, INTERVAL 1 HOUR)
  UNION ALL
  SELECT
    'post_profile_restore_v1' AS document_table,
    submission_timestamp,
    normalized_channel,
    sample_id,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
    ping_info.experiments,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.post_profile_restore_v1`
  WHERE
    submission_timestamp >= @window_start
    AND submission_timestamp < TIMESTAMP_ADD(@window_start, INTERVAL 1 HOUR)
  UNION ALL
  SELECT
    'profile_restore_v1' AS document_table,
    submission_timestamp,
    normalized_channel,
    sample_id,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
    ping_info.experiments,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.profile_restore_v1`
  WHERE
    submission_timestamp >= @window_start
    AND submission_timestamp < TIMESTAMP_ADD(@window_start, INTERVAL 1 HOUR)
  UNION ALL
  SELECT
    'profiles_v1' AS document_table,
    submission_timestamp,
    normalized_channel,
    sample_id,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
    ping_info.experiments,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.profiles_v1`
  WHERE
    submission_timestamp >= @window_start
    AND submission_timestamp < TIMESTAMP_ADD(@window_start, INTERVAL 1 HOUR)
  UNION ALL
  SELECT
    'prototype_no_code_events_v1' AS document_table,
    submission_timestamp,
    normalized_channel,
    sample_id,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
    ping_info.experiments,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.prototype_no_code_events_v1`
  WHERE
    submission_timestamp >= @window_start
    AND submission_timestamp < TIMESTAMP_ADD(@window_start, INTERVAL 1 HOUR)
  UNION ALL
  SELECT
    'sync_v1' AS document_table,
    submission_timestamp,
    normalized_channel,
    sample_id,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
    ping_info.experiments,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.sync_v1`
  WHERE
    submission_timestamp >= @window_start
    AND submission_timestamp < TIMESTAMP_ADD(@window_start, INTERVAL 1 HOUR)
  UNION ALL
  SELECT
    'urlbar_potential_exposure_v1' AS document_table,
    submission_timestamp,
    normalized_channel,
    sample_id,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
    ping_info.experiments,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.urlbar_potential_exposure_v1`
  WHERE
    submission_timestamp >= @window_start
    AND submission_timestamp < TIMESTAMP_ADD(@window_start, INTERVAL 1 HOUR)
),
pings_with_experiments AS (
  SELECT
    submission_timestamp,
    country,
    channel,
    version,
    -- Collapse each ping's events into counts per (category, name, extra key) so that
    -- repeated events are only expanded once per experiment
    ARRAY(
      SELECT AS STRUCT
        event.category AS event_category,
        event.name AS event_name,
        event_extra_key,
        COUNT(*) AS n,
      FROM
        UNNEST(events) AS event
      CROSS JOIN
        -- Add * extra to every event to get total event count
        UNNEST(ARRAY_CONCAT(ARRAY(SELECT key FROM UNNEST(event.extra)), ['*'])) AS event_extra_key
      WHERE
        -- See https://mozilla-hub.atlassian.net/browse/DENG-9732
        (
          document_table = 'events_v1'
          AND normalized_channel = 'release'
          AND event.category = 'uptake.remotecontent.result'
          AND event.name IN ('uptake_remotesettings', 'uptake_normandy')
          AND CAST(REGEXP_EXTRACT(version, r"^([0-9]+)") AS NUMERIC) >= 143
          AND sample_id != 0
        ) IS NOT TRUE
      GROUP BY
        event_category,
        event_name,
        event_extra_key
    ) AS event_counts,
    experiments_expanded,
    experiments_json,
    FARM_FINGERPRINT(experiments_json) AS experiments_fp,
  FROM
    (
      SELECT
        *,
        TO_JSON_STRING(experiments_expanded) AS experiments_json,
      FROM
        (
          SELECT
            *,
            -- One entry per experiment plus a '*' entry for aggregating across all experiments
            ARRAY_CONCAT(
              ARRAY(
                SELECT AS STRUCT
                  COALESCE(experiment.key, '*') AS experiment,
                  COALESCE(experiment.value.branch, '*') AS experiment_branch,
                FROM
                  UNNEST(experiments) AS experiment
                  WITH OFFSET
                ORDER BY
                  offset
              ),
              [STRUCT('*' AS experiment, '*' AS experiment_branch)]
            ) AS experiments_expanded,
          FROM
            pings
        )
    )
),
-- Collapse pings that share the same experiment set before expanding experiments
event_counts AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    country,
    channel,
    version,
    experiments_fp,
    event.event_category,
    event.event_name,
    event.event_extra_key,
    SUM(event.n) AS total_events,
  FROM
    pings_with_experiments
  CROSS JOIN
    UNNEST(event_counts) AS event
  GROUP BY
    window_start,
    country,
    channel,
    version,
    experiments_fp,
    event_category,
    event_name,
    event_extra_key
),
experiment_sets AS (
  SELECT
    experiments_fp,
    -- Fail instead of silently merging experiment sets if two ever share a fingerprint
    IF(
      MIN(experiments_json) = MAX(experiments_json),
      ANY_VALUE(experiments_expanded),
      ERROR(FORMAT('experiments_fp collision for %d', experiments_fp))
    ) AS experiments_expanded,
  FROM
    pings_with_experiments
  GROUP BY
    experiments_fp
)
SELECT
  DATE(window_start) AS submission_date,
  window_start,
  TIMESTAMP_ADD(window_start, INTERVAL 1 HOUR) AS window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  'Firefox for Desktop' AS normalized_app_name,
  channel,
  version,
  experiment.experiment,
  experiment.experiment_branch,
  SUM(total_events) AS total_events,
FROM
  event_counts
JOIN
  experiment_sets
  USING (experiments_fp)
CROSS JOIN
  UNNEST(experiments_expanded) AS experiment
GROUP BY
  submission_date,
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  channel,
  version,
  experiment,
  experiment_branch
