DECLARE submission_date DATE DEFAULT CURRENT_DATE() - 1;

SET submission_date = @submission_date;

-- Raw spans: expand glean pings into (trace_id, span_id, parent_span_id, events)
WITH raw_spans AS (
  SELECT
    submission_timestamp,
    JSON_VALUE(span, '$.trace_id') AS trace_id,
    JSON_VALUE(span, '$.span_id') AS span_id,
    JSON_VALUE(span, '$.parent_span_id') AS parent_span_id,
    JSON_QUERY_ARRAY(span, '$.events') AS events
  FROM
    ping
  CROSS JOIN
    UNNEST(JSON_QUERY_ARRAY(resource_spans)) AS resource_span
  CROSS JOIN
    UNNEST(JSON_QUERY_ARRAY(resource_span, '$.scope_spans')) AS scope_span
  CROSS JOIN
    UNNEST(JSON_QUERY_ARRAY(scope_span, '$.spans')) AS span
  WHERE
    DATE(submission_timestamp) = submission_date
),
-- Per-event hashes: unnest events and compute signature
event_hashes AS (
  SELECT
    submission_timestamp,
    e AS event_json,
    COALESCE(JSON_VALUE(e, '$.attributes["source.file"]'), '') AS source_file,
    SAFE_CAST(JSON_VALUE(e, '$.attributes["source.line"]') AS INT64) AS source_line,
    COALESCE(JSON_VALUE(e, '$.attributes.result'), '') AS result,
    SHA256(
      CONCAT(
        COALESCE(JSON_VALUE(e, '$.attributes["source.file"]'), ''),
        '\x00',
        COALESCE(JSON_VALUE(e, '$.attributes["source.line"]'), ''),
        '\x00',
        COALESCE(JSON_VALUE(e, '$.attributes.result'), '')
      )
    ) AS event_signature
  FROM
    raw_spans
  CROSS JOIN
    UNNEST(raw_spans.events) AS e
),
-- Aggregate events by signature
event_aggregates AS (
  SELECT
    submission_date AS submission_date,
    event_signature,
    source_file,
    source_line,
    result,
    COUNT(*) AS hit_count,
    MIN(DATE(submission_timestamp)) AS first_seen_date,
    MAX(DATE(submission_timestamp)) AS latest_seen_date
  FROM
    event_hashes
  GROUP BY
    submission_date,
    event_signature,
    source_file,
    source_line,
    result
)
-- MERGE into target table
MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1` AS T
USING
  event_aggregates AS S
ON
  T.event_signature = S.event_signature
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    submission_date,
    stable_event_id,
    event_signature,
    source_file,
    source_line,
    result,
    hit_count,
    first_seen_date,
    latest_seen_date
  )
  VALUES (
    S.submission_date,
    FARM_FINGERPRINT(GENERATE_UUID()),
    S.event_signature,
    S.source_file,
    S.source_line,
    S.result,
    S.hit_count,
    S.first_seen_date,
    S.latest_seen_date
  )
WHEN MATCHED THEN
  UPDATE SET
    hit_count = T.hit_count + S.hit_count,
    latest_seen_date = GREATEST(T.latest_seen_date, S.latest_seen_date);
