-- todo: make a script


-- Generated via ./bqetl generate glean_usage
-- This table aggregates event flows across Glean applications.
{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] not in ["telemetry", "accounts_frontend", "accounts_backend"] %}  
  {% if not outer_loop.first -%}
  UNION ALL
  {% endif -%}


DECLARE dummy INT64; -- dummy variable to indicate to bigquery-etl that this is a script
CREATE TEMP TABLE event_flows(
  submission_date DATE,
  document_namespace STRING,
  document_type STRING,
  document_version STRING,
  ping_count INT64
) AS (
 WITH dedup_events AS (
    SELECT
      distinct
      "2023-12-08" AS submission_date,  -- @submission_date
      ext.value AS flow_id,
      event.category AS category,
      event.name AS name,
      TIMESTAMP_ADD(
        SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time),
        -- limit event.timestamp, otherwise this will cause an overflow
        INTERVAL LEAST(event.timestamp, 20000000000000) MILLISECOND
      ) AS timestamp
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_live.events_v1`,
      UNNEST(events) AS event,
      UNNEST(event.extra) AS ext
    WHERE DATE(submission_timestamp) = "2023-12-08" -- @submission_date
    AND sample_id = 0 --
    AND ext.key = "flow_id"
  ),
  
  new_event_flows AS (
    SELECT
      "2023-12-08" AS submission_date,  -- @submission_date
      flow_id,
      ARRAY_AGG(
        STRUCT(
          category AS category,
          name AS name,
          timestamp AS timestamp
        )
        order by timestamp
      ) AS events
    FROM
      dedup_events
    GROUP BY flow_id
  ), unnested_events AS (
    SELECT
        new_event_flows.*,
        event,
        event_offset
      FROM
        new_event_flows,
        UNNEST(events) AS event WITH OFFSET AS event_offset
  ), source_target_events AS (
    SELECT 
      prev_event.flow_id,
      ARRAY_AGG(
        STRUCT(
          prev_event.event AS source,
          cur_event.event AS target
        ) 
        order by prev_event.event.timestamp
      ) AS events
    FROM
      unnested_events AS prev_event
    LEFT JOIN
      unnested_events AS cur_event
    ON prev_event.flow_id = cur_event.flow_id AND prev_event.event_offset = cur_event.event_offset - 1
    GROUP BY flow_id
  )
  SELECT 
    flow_id,
    ARRAY_AGG(
      event
      ORDER BY event.source.timestamp
    ) AS events
  FROM 
    (SELECT flow_id, event FROM
      source_target_events, UNNEST(events) AS event
      -- UNION ALL
      -- SELECT flow_id, event FROM source_target_events, UNNEST(events) AS event
      )
  GROUP BY flow_id

  -- SELECT
    
  -- FROM new_event_flows, UNNEST(events) AS event

  -- ), merged_event_flows AS (
  --   SELECT
  --     new_event_flows.submission_date AS submission_date,
  --     ARRAY_CONCAT_AGG(expr)
  --   FROM
  --     new_event_flows
  --   LEFT JOIN
  --     tmp.event_flow_monitoring_v1 AS prev_event_flows
  --   USING(flow_id)
  --   WHERE
  --     prev_event_flows.submission_date >= DATE_SUB("2023-12-08", INTERVAL 2 DAY)
  -- )
  -- SELECT * FROM new_event_flows
-- )



-- looker data
-- source - destination 


{% endif %}
{% endfor %}
{% endfor %}
