CREATE TABLE IF NOT EXISTS
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1`(
    submission_date DATE,
    stable_event_id STRING,
    event_signature STRING,
    source_file STRING,
    source_line INT64,
    result STRING,
    hit_count INT64
  )
CLUSTER BY
  event_signature;

MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1` AS T
  USING (
    WITH raw_spans AS (
      SELECT
        JSON_VALUE(span, '$.trace_id') AS trace_id,
        JSON_VALUE(span, '$.span_id') AS span_id,
        JSON_VALUE(span, '$.parent_span_id') AS parent_span_id,
        JSON_QUERY_ARRAY(span, '$.events') AS events
      FROM
        `{{ target_project }}.{{ app_id }}_stable.{{ ping_name }}_v1`
      CROSS JOIN
        UNNEST(JSON_QUERY_ARRAY(resource_spans)) AS resource_span
      CROSS JOIN
        UNNEST(JSON_QUERY_ARRAY(resource_span, '$.scope_spans')) AS scope_span
      CROSS JOIN
        UNNEST(JSON_QUERY_ARRAY(scope_span, '$.spans')) AS span
      WHERE
        DATE(submission_timestamp) = @submission_date
    ),
    event_hashes AS (
      SELECT
        e AS event_json,
        COALESCE(JSON_VALUE(e, '$.attributes["source.file"]'), '') AS source_file,
        SAFE_CAST(JSON_VALUE(e, '$.attributes["source.line"]') AS INT64) AS source_line,
        COALESCE(JSON_VALUE(e, '$.attributes.result'), '') AS result,
        TO_BASE64(
          SHA256(
            CONCAT(
              COALESCE(JSON_VALUE(e, '$.attributes["source.file"]'), ''),
              '\x00',
              COALESCE(JSON_VALUE(e, '$.attributes["source.line"]'), ''),
              '\x00',
              COALESCE(JSON_VALUE(e, '$.attributes.result'), '')
            )
          )
        ) AS event_signature
      FROM
        raw_spans
      CROSS JOIN
        UNNEST(raw_spans.events) AS e
    ),
    event_aggregates AS (
      SELECT
        @submission_date AS submission_date,
        event_signature,
        source_file,
        source_line,
        result,
        COUNT(*) AS hit_count
      FROM
        event_hashes
      GROUP BY
        event_signature,
        source_file,
        source_line,
        result
    )
    SELECT
      *
    FROM
      event_aggregates
  ) AS S
  ON T.event_signature = S.event_signature
  AND T.submission_date = S.submission_date
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (submission_date, stable_event_id, event_signature, source_file, source_line, result, hit_count)
  VALUES
    (
      S.submission_date,
      GENERATE_UUID(),
      S.event_signature,
      S.source_file,
      S.source_line,
      S.result,
      S.hit_count
    )
  WHEN MATCHED
THEN
  UPDATE
  SET
    hit_count = S.hit_count;