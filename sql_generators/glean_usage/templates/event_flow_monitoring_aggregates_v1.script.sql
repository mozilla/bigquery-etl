-- Generated via ./bqetl generate glean_usage
-- This table aggregates event flows across Glean applications.
DECLARE dummy INT64; -- dummy variable to indicate to bigquery-etl that this is a script
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
      {% for app in apps -%}
        {% if not loop.first -%}
          UNION ALL
        {% endif %}
        {% if dataset_id not in ["telemetry", "accounts_frontend", "accounts_backend"] %}
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
            "{{ app }}" AS normalized_app_name,
            client_info.app_channel AS channel
          FROM
            `moz-fx-data-shared-prod.{{ app }}.events_unnested`,
            UNNEST(event_extra) AS ext
          WHERE
            DATE(submission_timestamp) = @submission_date
            AND ext.key = "flow_id"
        {% elif dataset_id in ["accounts_frontend", "accounts_backend"] %}
          SELECT DISTINCT
            @submission_date AS submission_date,
            metrics.string.session_flow_id AS flow_id,
            NULL AS category,
            metrics.string.event_name AS name,
            submission_timestamp AS timestamp,
            "{{ app }}" AS normalized_app_name,
            client_info.app_channel AS channel
          FROM
            `moz-fx-data-shared-prod.{{ app }}.accounts_events`
          WHERE
            DATE(submission_timestamp) = @submission_date
            AND metrics.string.session_flow_id != ""
        {% endif %}
      {% endfor %}
    ),
    -- determine events that belong to the same flow
    new_event_flows AS (
      SELECT
        @submission_date AS submission_date,
        flow_id,
        normalized_app_name,
        channel,
        ARRAY_AGG(
          STRUCT(category AS category, name AS name, timestamp AS timestamp)
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
            CONCAT(event.source.category, ".", event.source.name)
            ORDER BY
              event.source.timestamp
          ),
          [
            ARRAY_REVERSE(
              ARRAY_AGG(
                CONCAT(event.target.category, ".", event.target.name)
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
          `{{ project_id }}.{{ target_table }}`,
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
  `{{ project_id }}.{{ target_table }}` r
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
