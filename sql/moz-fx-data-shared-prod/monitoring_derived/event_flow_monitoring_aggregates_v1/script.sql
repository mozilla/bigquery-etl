-- Generated via ./bqetl generate glean_usage
-- This table aggregates event flows across Glean applications.
DECLARE dummy INT64; -- dummy variable to indicate to BigQuery this is a script
CREATE TEMP TABLE
  event_flows(
    submission_date DATE,
    flow_id STRING,
    normalized_app_name STRING,
    channel STRING,
    events ARRAY<
      STRUCT<
        source STRUCT<category STRING, name STRING, timestamp TIMESTAMP>,
        target STRUCT<category STRING, name STRING, timestamp TIMESTAMP>
      >
    >,
    flow_hash STRING
  ) AS (
    -- get events from all apps that are related to some flow (have 'flow_id' in event_extras)
    WITH all_app_events AS (
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "firefox_desktop" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_desktop.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "firefox_desktop_background_update" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_background_update.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "firefox_desktop_background_defaultagent" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_background_defaultagent.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "pine" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.pine.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "fenix" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.fenix.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "firefox_ios" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "reference_browser" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.reference_browser.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "firefox_fire_tv" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_fire_tv.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "firefox_reality" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_reality.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "lockwise_android" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.lockwise_android.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "lockwise_ios" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.lockwise_ios.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "mozregression" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.mozregression.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "burnham" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.burnham.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "mozphab" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.mozphab.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "firefox_echo_show" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_echo_show.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "firefox_reality_pc" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_reality_pc.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "mach" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.mach.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "focus_ios" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.focus_ios.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "klar_ios" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.klar_ios.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "focus_android" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.focus_android.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "klar_android" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.klar_android.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "bergamot" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.bergamot.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "firefox_translations" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_translations.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "mozilla_vpn" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.mozilla_vpn.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "mozillavpn_backend_cirrus" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.mozillavpn_backend_cirrus.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "glean_dictionary" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.glean_dictionary.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "mdn_yari" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.mdn_yari.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "bedrock" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.bedrock.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "viu_politica" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.viu_politica.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "treeherder" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.treeherder.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "firefox_desktop_background_tasks" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_background_tasks.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "accounts_frontend" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.accounts_frontend.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "accounts_backend" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.accounts_backend.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "monitor_cirrus" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.monitor_cirrus.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "debug_ping_view" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.debug_ping_view.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "monitor_frontend" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.monitor_frontend.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "moso_mastodon_backend" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.moso_mastodon_backend.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "moso_mastodon_web" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.moso_mastodon_web.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "tiktokreporter_ios" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.tiktokreporter_ios.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
      SELECT DISTINCT
        @submission_date AS submission_date,
        ext.value AS flow_id,
        event_category AS category,
        event_name AS name,
        TIMESTAMP_ADD(
          submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
          INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
        ) AS timestamp,
        "tiktokreporter_android" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.tiktokreporter_android.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
    ),
    -- determine events that belong to the same flow
    new_event_flows AS (
      SELECT
        @submission_date AS submission_date,
        flow_id,
        normalized_app_name,
        channel,
        ARRAY_AGG(
          (
            SELECT AS STRUCT
              category AS category,
              name AS name,
              timestamp AS timestamp
            LIMIT
              100 -- limit number of events considered
          )
          ORDER BY
            timestamp
        ) AS events
      FROM
        all_app_events
      GROUP BY
        flow_id,
        normalized_app_name,
        channel
    ),
    unnested_events AS (
      SELECT
        new_event_flows.*,
        event,
        event_offset
      FROM
        new_event_flows,
        UNNEST(events) AS event
        WITH OFFSET AS event_offset
    ),
    -- create source -> target event pairs based on the order of when the events were seen
    source_target_events AS (
      SELECT
        prev_event.flow_id,
        prev_event.normalized_app_name,
        prev_event.channel,
        ARRAY_AGG(
          STRUCT(prev_event.event AS source, cur_event.event AS target)
          ORDER BY
            prev_event.event.timestamp
        ) AS events
      FROM
        unnested_events AS prev_event
      INNER JOIN
        unnested_events AS cur_event
      ON
        prev_event.flow_id = cur_event.flow_id
        AND prev_event.event_offset = cur_event.event_offset - 1
      GROUP BY
        flow_id,
        normalized_app_name,
        channel
    )
    SELECT
      @submission_date AS submission_date,
      flow_id,
      normalized_app_name,
      channel,
      ARRAY_AGG(event ORDER BY event.source.timestamp) AS events,
      -- create a flow hash that concats all the events that are part of the flow
      -- <event_category>.<event_name> -> <event_category>.<event_name> -> ...
      ARRAY_TO_STRING(
        ARRAY_CONCAT(
          ARRAY_AGG(
            CONCAT(
              IF(event.source.category IS NOT NULL, CONCAT(event.source.category, "."), ""),
              event.source.name
            )
            ORDER BY
              event.source.timestamp
          ),
          [
            ARRAY_REVERSE(
              ARRAY_AGG(
                CONCAT(
                  IF(event.target.category IS NOT NULL, CONCAT(event.target.category, "."), ""),
                  event.target.name
                )
                ORDER BY
                  event.source.timestamp
              )
            )[SAFE_OFFSET(0)]
          ]
        ),
        " -> "
      ) AS flow_hash
    FROM
      (
        SELECT
          flow_id,
          normalized_app_name,
          channel,
          event
        FROM
          source_target_events,
          UNNEST(events) AS event
        UNION ALL
        -- some flows might go over multiple days;
        -- use previously seen flows and combine with new flows
        SELECT
          flow_id,
          normalized_app_name,
          channel,
          event
        FROM
          `moz-fx-data-shared-prod.monitoring_derived.event_flow_monitoring_aggregates_v1`,
          UNNEST(events) AS event
        WHERE
          submission_date > DATE_SUB(@submission_date, INTERVAL 3 DAY)
      )
    GROUP BY
      flow_id,
      normalized_app_name,
      channel
  );

MERGE
  `moz-fx-data-shared-prod.monitoring_derived.event_flow_monitoring_aggregates_v1` r
USING
  event_flows f
ON
  r.flow_id = f.flow_id
  -- look back up to 3 days to see if a flow has seen new events and needs to be replaced
  AND r.submission_date > DATE_SUB(@submission_date, INTERVAL 3 DAY)
WHEN NOT MATCHED
THEN
  INSERT
    (submission_date, flow_id, events, normalized_app_name, channel, flow_hash)
  VALUES
    (f.submission_date, f.flow_id, f.events, f.normalized_app_name, f.channel, f.flow_hash)
  WHEN NOT MATCHED BY SOURCE
    -- look back up to 3 days to see if a flow has seen new events and needs to be replaced
    AND r.submission_date > DATE_SUB(@submission_date, INTERVAL 3 DAY)
THEN
  DELETE;
