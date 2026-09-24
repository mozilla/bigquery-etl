CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_traces_v1`(
    trace_signature STRING,
    stable_trace_id STRING,
    trace_key STRING,
    first_seen_date DATE,
    last_seen_date DATE
  )
CLUSTER BY
  trace_signature;

MERGE INTO
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_traces_v1` AS T
  USING (
    WITH RECURSIVE raw_spans AS (
      SELECT
        submission_timestamp,
        JSON_VALUE(span, '$.trace_id') AS trace_id,
        JSON_VALUE(span, '$.span_id') AS span_id,
        JSON_VALUE(span, '$.parent_span_id') AS parent_span_id,
        JSON_QUERY_ARRAY(span, '$.events') AS events,
        SAFE_CAST(JSON_VALUE(span, '$.start_time_unix_nano') AS INT64) AS start_time_unix_nano,
        SAFE_CAST(JSON_VALUE(span, '$.end_time_unix_nano') AS INT64) AS end_time_unix_nano
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_stable.gecko_trace_v1`
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
    leaf_spans AS (
      SELECT
        rs.trace_id,
        rs.span_id,
        rs.parent_span_id
      FROM
        raw_spans rs
      WHERE
        rs.span_id NOT IN (SELECT parent_span_id FROM raw_spans WHERE parent_span_id IS NOT NULL)
    ),
    path_spans AS (
      SELECT
        trace_id,
        span_id AS leaf_span_id,
        span_id AS current_span_id,
        parent_span_id AS current_parent_span_id,
        1 AS depth
      FROM
        leaf_spans
      UNION ALL
      SELECT
        ps.trace_id,
        ps.leaf_span_id,
        rs.span_id AS current_span_id,
        rs.parent_span_id AS current_parent_span_id,
        ps.depth + 1
      FROM
        path_spans ps
      JOIN
        raw_spans rs
        ON ps.current_parent_span_id = rs.span_id
        AND ps.trace_id = rs.trace_id
    ),
    path_events AS (
      SELECT
        ps.trace_id,
        ps.leaf_span_id,
        seh.event_hash,
        ROW_NUMBER() OVER (
          PARTITION BY
            ps.trace_id,
            ps.leaf_span_id
          ORDER BY
            ps.depth DESC,
            seh.event_offset ASC
        ) AS event_position
      FROM
        path_spans ps
      JOIN
        span_event_hashes seh
        ON ps.current_span_id = seh.span_id
        AND ps.trace_id = seh.trace_id
    ),
    trace_signatures AS (
      SELECT
        trace_id,
        leaf_span_id,
        TO_BASE64(SHA256(STRING_AGG(event_hash, ',' ORDER BY event_position))) AS trace_signature
      FROM
        path_events
      GROUP BY
        trace_id,
        leaf_span_id
    ),
    trace_representatives AS (
      SELECT
        trace_signature,
        ANY_VALUE(STRUCT(trace_id, leaf_span_id)) AS instance
      FROM
        trace_signatures
      GROUP BY
        trace_signature
    ),
    -- Traces whose events are not yet in gecko_trace_events_v1 are skipped
    -- so that a partial event list never produces a wrong trace_key.
    trace_keys AS (
      SELECT
        tr.trace_signature,
        TO_BASE64(
          SHA256(STRING_AGG(ev.stable_event_id, ',' ORDER BY pe.event_position))
        ) AS trace_key
      FROM
        trace_representatives tr
      JOIN
        path_events pe
        ON pe.trace_id = tr.instance.trace_id
        AND pe.leaf_span_id = tr.instance.leaf_span_id
      LEFT JOIN
        `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_events_v1` ev
        ON pe.event_hash = ev.event_signature
      GROUP BY
        tr.trace_signature
      HAVING
        COUNTIF(ev.stable_event_id IS NULL) = 0
    ),
    existing_ids AS (
      SELECT
        trace_key,
        MIN(stable_trace_id) AS stable_trace_id
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_traces_v1`
      GROUP BY
        trace_key
    ),
    new_ids AS (
      SELECT
        trace_key,
        GENERATE_UUID() AS stable_trace_id
      FROM
        (SELECT DISTINCT trace_key FROM trace_keys)
    )
    SELECT
      tk.trace_signature,
      COALESCE(ex.stable_trace_id, nw.stable_trace_id) AS stable_trace_id,
      tk.trace_key
    FROM
      trace_keys tk
    LEFT JOIN
      existing_ids ex
      USING (trace_key)
    LEFT JOIN
      new_ids nw
      USING (trace_key)
  ) AS S
  ON T.trace_signature = S.trace_signature
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (trace_signature, stable_trace_id, trace_key, first_seen_date, last_seen_date)
  VALUES
    (S.trace_signature, S.stable_trace_id, S.trace_key, @submission_date, @submission_date)
  WHEN MATCHED
THEN
  UPDATE
  SET
    first_seen_date = LEAST(T.first_seen_date, @submission_date),
    last_seen_date = GREATEST(T.last_seen_date, @submission_date);
