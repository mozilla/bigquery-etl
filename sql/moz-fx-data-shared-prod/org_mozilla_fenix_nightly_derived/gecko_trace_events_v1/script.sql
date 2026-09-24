CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_events_v1`(
    event_signature STRING,
    stable_event_id STRING,
    source_file STRING,
    source_line INT64,
    result STRING,
    first_seen_date DATE,
    last_seen_date DATE
  )
CLUSTER BY
  event_signature;

MERGE INTO
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_events_v1` AS T
  USING (
    WITH raw_spans AS (
      SELECT
        submission_timestamp,
        JSON_VALUE(span, '$.trace_id') AS trace_id,
        JSON_VALUE(span, '$.span_id') AS span_id,
        JSON_VALUE(span, '$.parent_span_id') AS parent_span_id,
        JSON_QUERY_ARRAY(span, '$.events') AS events,
        SAFE_CAST(JSON_VALUE(span, '$.start_time_unix_nano') AS INT64) AS start_time_unix_nano,
        SAFE_CAST(JSON_VALUE(span, '$.end_time_unix_nano') AS INT64) AS end_time_unix_nano
      FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.gecko_trace_v1`
      CROSS JOIN
        UNNEST(
          JSON_QUERY_ARRAY(metrics.object.gecko_trace_traces_data, '$.resource_spans')
        ) AS resource_span
      CROSS JOIN
        UNNEST(JSON_QUERY_ARRAY(resource_span, '$.scope_spans')) AS scope_span
      CROSS JOIN
        UNNEST(JSON_QUERY_ARRAY(scope_span, '$.spans')) AS span
      WHERE
        DATE(submission_timestamp) = @submission_date
    ),
    span_event_hashes AS (
      SELECT
        rs.trace_id,
        rs.span_id,
        offset AS event_offset,
        COALESCE(JSON_VALUE(e, '$.attributes."source.file"'), '') AS source_file,
        SAFE_CAST(JSON_VALUE(e, '$.attributes."source.line"') AS INT64) AS source_line,
        COALESCE(JSON_VALUE(e, '$.attributes.result'), '') AS result,
        TO_BASE64(
          SHA256(
            CONCAT(
              COALESCE(JSON_VALUE(e, '$.attributes."source.file"'), ''),
              '\x00',
              COALESCE(JSON_VALUE(e, '$.attributes."source.line"'), ''),
              '\x00',
              COALESCE(JSON_VALUE(e, '$.attributes.result'), '')
            )
          )
        ) AS event_hash
      FROM
        raw_spans rs
      CROSS JOIN
        UNNEST(rs.events) AS e
        WITH OFFSET AS offset
    ),
    daily_events AS (
      SELECT
        event_hash AS event_signature,
        ANY_VALUE(source_file) AS source_file,
        ANY_VALUE(source_line) AS source_line,
        ANY_VALUE(result) AS result
      FROM
        span_event_hashes
      GROUP BY
        event_hash
    )
    SELECT
      *
    FROM
      daily_events
  ) AS S
  ON T.event_signature = S.event_signature
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (
      event_signature,
      stable_event_id,
      source_file,
      source_line,
      result,
      first_seen_date,
      last_seen_date
    )
  VALUES
    (
      S.event_signature,
      GENERATE_UUID(),
      S.source_file,
      S.source_line,
      S.result,
      @submission_date,
      @submission_date
    )
  WHEN MATCHED
THEN
  UPDATE
  SET
    first_seen_date = LEAST(T.first_seen_date, @submission_date),
    last_seen_date = GREATEST(T.last_seen_date, @submission_date);
