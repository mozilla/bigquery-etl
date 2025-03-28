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
        "Firefox for Desktop" AS normalized_app_name,
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
        "Firefox Crash Reporter" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_crashreporter.events_unnested`,
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
        "Firefox for Desktop Background Update Task" AS normalized_app_name,
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
        "Firefox Desktop Default Agent Task" AS normalized_app_name,
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
        "Pinebuild" AS normalized_app_name,
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
        "Firefox for Android" AS normalized_app_name,
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
        "Firefox for iOS" AS normalized_app_name,
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
        "Reference Browser" AS normalized_app_name,
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
        "Firefox for Fire TV" AS normalized_app_name,
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
        "Firefox Reality" AS normalized_app_name,
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
        "Lockwise for Android" AS normalized_app_name,
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
        "Lockwise for iOS" AS normalized_app_name,
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
        "Burnham" AS normalized_app_name,
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
        "Firefox for Echo Show" AS normalized_app_name,
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
        "Firefox Reality for PC-connected VR platforms" AS normalized_app_name,
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
        "Firefox Focus for iOS" AS normalized_app_name,
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
        "Firefox Klar for iOS" AS normalized_app_name,
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
        "Firefox Focus for Android" AS normalized_app_name,
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
        "Firefox Klar for Android" AS normalized_app_name,
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
        "Bergamot Translator" AS normalized_app_name,
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
        "Firefox Translations" AS normalized_app_name,
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
        "Mozilla VPN" AS normalized_app_name,
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
        "Mozilla VPN Cirrus Sidecar" AS normalized_app_name,
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
        "Glean Dictionary" AS normalized_app_name,
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
        "Mozilla Developer Network" AS normalized_app_name,
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
        "www.mozilla.org" AS normalized_app_name,
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
        "Viu Politica" AS normalized_app_name,
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
        "Treeherder" AS normalized_app_name,
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
        "Firefox Desktop background tasks" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_background_tasks.events_unnested`,
        UNNEST(event_extra) AS ext
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND ext.key = "flow_id"
      UNION ALL
        (
          WITH events_unnested_with_metrics AS (
            -- events_unnested views do not have metrics, accounts send flow_id in a string metric
            -- so we need to unnest with metrics here
            SELECT
              e.* EXCEPT (events),
              event.timestamp AS event_timestamp,
              event.category AS event_category,
              event.name AS event_name,
              event.extra AS event_extra
            FROM
              `moz-fx-data-shared-prod.accounts_frontend.events` e
            CROSS JOIN
              UNNEST(e.events) AS event
          )
          SELECT DISTINCT
            @submission_date AS submission_date,
            metrics.string.session_flow_id AS flow_id,
            event_category AS category,
            event_name AS name,
            TIMESTAMP_ADD(
              submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
              INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
            ) AS timestamp,
            "Mozilla Accounts Frontend" AS normalized_app_name,
            client_info.app_channel AS channel
          FROM
            events_unnested_with_metrics
          WHERE
            DATE(submission_timestamp) = @submission_date
            AND metrics.string.session_flow_id IS NOT NULL
            AND metrics.string.session_flow_id != ""
        )
      UNION ALL
        (
          WITH events_unnested_with_metrics AS (
            -- events_unnested views do not have metrics, accounts send flow_id in a string metric
            -- so we need to unnest with metrics here
            SELECT
              e.* EXCEPT (events),
              event.timestamp AS event_timestamp,
              event.category AS event_category,
              event.name AS event_name,
              event.extra AS event_extra
            FROM
              `moz-fx-data-shared-prod.accounts_backend.events` e
            CROSS JOIN
              UNNEST(e.events) AS event
          )
          SELECT DISTINCT
            @submission_date AS submission_date,
            metrics.string.session_flow_id AS flow_id,
            event_category AS category,
            event_name AS name,
            TIMESTAMP_ADD(
              submission_timestamp,
          -- limit event.timestamp, otherwise this will cause an overflow
              INTERVAL LEAST(event_timestamp, 20000000000000) MILLISECOND
            ) AS timestamp,
            "Mozilla Accounts Backend" AS normalized_app_name,
            client_info.app_channel AS channel
          FROM
            events_unnested_with_metrics
          WHERE
            DATE(submission_timestamp) = @submission_date
            AND metrics.string.session_flow_id IS NOT NULL
            AND metrics.string.session_flow_id != ""
        )
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
        "Mozilla Accounts (Cirrus)" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.accounts_cirrus.events_unnested`,
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
        "Mozilla Monitor (Cirrus)" AS normalized_app_name,
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
        "Glean Debug Ping Viewer" AS normalized_app_name,
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
        "Mozilla Monitor (Frontend)" AS normalized_app_name,
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
        "Mozilla Monitor (Backend)" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.monitor_backend.events_unnested`,
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
        "Firefox Relay Backend" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.relay_backend.events_unnested`,
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
        "Glean.js Documentation" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.gleanjs_docs.events_unnested`,
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
        "Mozilla Ads Backend Service" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.ads_backend.events_unnested`,
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
        "Thunderbird" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.thunderbird_desktop.events_unnested`,
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
        "Thunderbird for Android" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.thunderbird_android.events_unnested`,
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
        "Sync Storage" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.syncstorage.events_unnested`,
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
        "GLAM" AS normalized_app_name,
        client_info.app_channel AS channel
      FROM
        `moz-fx-data-shared-prod.glam.events_unnested`,
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
        ON prev_event.flow_id = cur_event.flow_id
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
  USING event_flows f
  ON r.flow_id = f.flow_id
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
