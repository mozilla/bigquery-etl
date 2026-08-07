CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_platform_counts_v1`(
    submission_date DATE,
    stable_trace_id STRING,
    trace_signature STRING,
    app_build STRING,
    normalized_os STRING,
    normalized_os_version STRING,
    architecture STRING,
    hit_count INT64
  )
CLUSTER BY
  trace_signature;

MERGE INTO
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_platform_counts_v1` AS T
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
        SAFE_CAST(JSON_VALUE(span, '$.end_time_unix_nano') AS INT64) AS end_time_unix_nano,
        client_info.app_build AS app_build,
        normalized_os,
        normalized_os_version,
        client_info.architecture AS architecture
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_stable.gecko_trace_v1`
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
    platform_counts AS (
      SELECT
        @submission_date AS submission_date,
        tr.stable_trace_id,
        ts.trace_signature,
        rs.app_build,
        rs.normalized_os,
        rs.normalized_os_version,
        rs.architecture,
        COUNT(DISTINCT ts.trace_id) AS hit_count
      FROM
        trace_signatures ts
      JOIN
        raw_spans rs
        ON ts.trace_id = rs.trace_id
      JOIN
        `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_traces_v1` tr
        ON ts.trace_signature = tr.trace_signature
        AND tr.submission_date = @submission_date
      GROUP BY
        tr.stable_trace_id,
        ts.trace_signature,
        rs.app_build,
        rs.normalized_os,
        rs.normalized_os_version,
        rs.architecture
    )
    SELECT
      *
    FROM
      platform_counts
  ) AS S
  ON T.stable_trace_id = S.stable_trace_id
  AND T.submission_date = S.submission_date
  AND T.app_build = S.app_build
  AND T.normalized_os = S.normalized_os
  AND T.normalized_os_version = S.normalized_os_version
  AND T.architecture = S.architecture
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (
      submission_date,
      stable_trace_id,
      trace_signature,
      app_build,
      normalized_os,
      normalized_os_version,
      architecture,
      hit_count
    )
  VALUES
    (
      S.submission_date,
      S.stable_trace_id,
      S.trace_signature,
      S.app_build,
      S.normalized_os,
      S.normalized_os_version,
      S.architecture,
      S.hit_count
    )
  WHEN MATCHED
THEN
  UPDATE
  SET
    trace_signature = S.trace_signature,
    hit_count = S.hit_count;
