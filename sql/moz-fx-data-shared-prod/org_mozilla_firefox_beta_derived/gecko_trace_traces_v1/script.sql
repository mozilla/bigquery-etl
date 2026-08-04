CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_traces_v1`(
    submission_date DATE,
    stable_trace_id STRING,
    trace_signature STRING,
    hit_count INT64,
    avg_duration_nano FLOAT64
  )
CLUSTER BY
  trace_signature;

MERGE INTO
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_traces_v1` AS T
  USING (
    WITH RECURSIVE
-- Shared CTEs: raw span extraction, per-event hashing, leaf-to-root span
-- walk, ordered event expansion, and trace signature computation.
-- Included by traces_v1 and trace_events_v1 scripts.
    raw_spans AS (
      SELECT
        submission_timestamp,
        JSON_VALUE(span, '$.trace_id') AS trace_id,
        JSON_VALUE(span, '$.span_id') AS span_id,
        JSON_VALUE(span, '$.parent_span_id') AS parent_span_id,
        JSON_QUERY_ARRAY(span, '$.events') AS events,
        SAFE_CAST(JSON_VALUE(span, '$.start_time_unix_nano') AS INT64) AS start_time_unix_nano,
        SAFE_CAST(JSON_VALUE(span, '$.end_time_unix_nano') AS INT64) AS end_time_unix_nano
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.gecko_trace_v1`
      CROSS JOIN
        UNNEST(JSON_QUERY_ARRAY(resource_spans)) AS resource_span
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
    trace_instances AS (
      SELECT
        ts.trace_id,
        ts.trace_signature,
        MAX(rs.end_time_unix_nano) - MIN(rs.start_time_unix_nano) AS duration_nano
      FROM
        trace_signatures ts
      JOIN
        raw_spans rs
        ON ts.trace_id = rs.trace_id
      GROUP BY
        ts.trace_id,
        ts.trace_signature
    ),
    trace_aggregates AS (
      SELECT
        @submission_date AS submission_date,
        trace_signature,
        COUNT(*) AS hit_count,
        AVG(CAST(duration_nano AS FLOAT64)) AS avg_duration_nano
      FROM
        trace_instances
      GROUP BY
        trace_signature
    )
    SELECT
      *
    FROM
      trace_aggregates
  ) AS S
  ON T.trace_signature = S.trace_signature
  AND T.submission_date = S.submission_date
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (submission_date, stable_trace_id, trace_signature, hit_count, avg_duration_nano)
  VALUES
    (S.submission_date, GENERATE_UUID(), S.trace_signature, S.hit_count, S.avg_duration_nano)
  WHEN MATCHED
THEN
  UPDATE
  SET
    hit_count = S.hit_count,
    avg_duration_nano = S.avg_duration_nano;
