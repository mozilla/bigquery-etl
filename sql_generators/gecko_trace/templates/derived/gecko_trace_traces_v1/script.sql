DECLARE submission_date DATE DEFAULT CURRENT_DATE() - 1;

SET submission_date = @submission_date;

-- Raw spans: expand glean pings into (trace_id, span_id, parent_span_id, events, timing)
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
-- Flag spans that have at least one event
span_has_events AS (
  SELECT
    rs.trace_id,
    rs.span_id,
    ARRAY_LENGTH(rs.events) > 0 AS has_events
  FROM
    raw_spans rs
),
-- Recursive CTE: find effective parent (nearest ancestor with events)
-- for each span, making zero-event spans transparent
span_effective_parent AS (
  -- Base: roots and spans whose parent has events
  SELECT
    rs.trace_id,
    rs.span_id,
    CASE
      WHEN rs.parent_span_id IS NULL THEN NULL
      WHEN sha.has_events THEN rs.parent_span_id
      ELSE NULL -- will recurse in next case
    END AS effective_parent_span_id,
    1 AS depth
  FROM
    raw_spans rs
  LEFT JOIN
    span_has_events sha ON rs.parent_span_id = sha.span_id AND rs.trace_id = sha.trace_id
  WHERE
    rs.parent_span_id IS NULL OR sha.has_events
  UNION ALL
  -- Recursive: span whose parent has no events; climb to parent's effective parent
  SELECT
    rs.trace_id,
    rs.span_id,
    sep.effective_parent_span_id,
    sep.depth + 1
  FROM
    raw_spans rs
  JOIN
    span_has_events sha ON rs.parent_span_id = sha.span_id AND rs.trace_id = sha.trace_id
  JOIN
    span_effective_parent sep ON rs.parent_span_id = sep.span_id AND rs.trace_id = sep.trace_id
  WHERE
    NOT sha.has_events
),
-- Per-event hashes and offsets for Merkle chaining
event_hash_rows AS (
  SELECT
    rs.trace_id,
    rs.span_id,
    rs.parent_span_id,
    e AS event_json,
    OFFSET + 1 AS event_offset,
    ARRAY_LENGTH(rs.events) AS span_event_count,
    SHA256(
      CONCAT(
        COALESCE(JSON_VALUE(e, '$.attributes["source.file"]'), ''),
        '\x00',
        COALESCE(JSON_VALUE(e, '$.attributes["source.line"]'), ''),
        '\x00',
        COALESCE(JSON_VALUE(e, '$.attributes.result'), '')
      )
    ) AS event_hash
  FROM
    raw_spans rs
  CROSS JOIN
    UNNEST(rs.events) AS e WITH OFFSET
),
-- Recursive CTE: Merkle-chain signatures per event
-- Each event gets a chain_sig = SHA256(prev_chain_sig || b'\x00' || event_hash)
event_chain AS (
  -- Base A: first event of a span with no ancestor events
  SELECT
    ehr.trace_id,
    ehr.span_id,
    0 AS parent_event_offset,
    ehr.event_offset,
    ehr.span_event_count,
    ehr.event_hash,
    SHA256(ehr.event_hash) AS chain_sig
  FROM
    event_hash_rows ehr
  LEFT JOIN
    span_effective_parent sep ON ehr.span_id = sep.span_id AND ehr.trace_id = sep.trace_id
  WHERE
    ehr.event_offset = 1 AND sep.effective_parent_span_id IS NULL
  UNION ALL
  -- Base B: first event of a span whose effective parent has events
  -- Seeded from parent's last event signature
  SELECT
    ehr.trace_id,
    ehr.span_id,
    parent_ec.event_offset AS parent_event_offset,
    ehr.event_offset,
    ehr.span_event_count,
    ehr.event_hash,
    SHA256(
      CONCAT(parent_ec.chain_sig, b'\x00', ehr.event_hash)
    ) AS chain_sig
  FROM
    event_hash_rows ehr
  JOIN
    span_effective_parent sep ON ehr.span_id = sep.span_id AND ehr.trace_id = sep.trace_id
  JOIN
    event_chain parent_ec ON sep.effective_parent_span_id = parent_ec.span_id
      AND ehr.trace_id = parent_ec.trace_id
      AND parent_ec.event_offset = parent_ec.span_event_count -- last event of parent
  WHERE
    ehr.event_offset = 1 AND sep.effective_parent_span_id IS NOT NULL
  UNION ALL
  -- Recursive: subsequent event in same span
  SELECT
    ehr.trace_id,
    ehr.span_id,
    prev_ec.event_offset AS parent_event_offset,
    ehr.event_offset,
    ehr.span_event_count,
    ehr.event_hash,
    SHA256(
      CONCAT(prev_ec.chain_sig, b'\x00', ehr.event_hash)
    ) AS chain_sig
  FROM
    event_hash_rows ehr
  JOIN
    event_chain prev_ec ON ehr.span_id = prev_ec.span_id
      AND ehr.trace_id = prev_ec.trace_id
      AND ehr.event_offset = prev_ec.event_offset + 1
)
OPTIONS (max_iterations = 1000),
-- Collect leaf event signatures (terminal events of spans with no children)
leaf_sigs AS (
  SELECT
    ec.trace_id,
    ARRAY_AGG(ec.chain_sig ORDER BY ec.chain_sig) AS leaf_chain_sigs
  FROM
    event_chain ec
  LEFT JOIN
    raw_spans child ON ec.span_id = child.parent_span_id AND ec.trace_id = child.trace_id
  WHERE
    ec.event_offset = ec.span_event_count -- last event of span
    AND child.span_id IS NULL -- no children
  GROUP BY
    ec.trace_id
),
-- Compute final trace signature by combining all leaf signatures
trace_signature_cte AS (
  SELECT
    ls.trace_id,
    SHA256(
      STRING_AGG(TO_BASE64(chain_sig) ORDER BY chain_sig)
    ) AS trace_signature
  FROM
    leaf_sigs ls
  CROSS JOIN
    UNNEST(ls.leaf_chain_sigs) AS chain_sig
  GROUP BY
    ls.trace_id
),
-- Aggregate trace instances for timestamps and duration
trace_instances AS (
  SELECT
    ts.trace_id,
    ts.trace_signature,
    MIN(rs.submission_timestamp) AS first_submission_timestamp,
    MAX(rs.submission_timestamp) AS latest_submission_timestamp,
    MAX(rs.end_time_unix_nano) - MIN(rs.start_time_unix_nano) AS duration_nano
  FROM
    trace_signature_cte ts
  JOIN
    raw_spans rs ON ts.trace_id = rs.trace_id
  GROUP BY
    ts.trace_id,
    ts.trace_signature
),
-- Aggregate by trace signature for hit counts and dates
trace_aggregates AS (
  SELECT
    @submission_date AS submission_date,
    trace_signature,
    COUNT(*) AS hit_count,
    MIN(DATE(first_submission_timestamp)) AS first_seen_date,
    MAX(DATE(latest_submission_timestamp)) AS latest_seen_date,
    AVG(CAST(duration_nano AS FLOAT64)) AS avg_duration_nano
  FROM
    trace_instances
  GROUP BY
    trace_signature
)
-- MERGE into target table
MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1` AS T
USING
  trace_aggregates AS S
ON
  T.trace_signature = S.trace_signature
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    submission_date,
    stable_trace_id,
    trace_signature,
    hit_count,
    first_seen_date,
    latest_seen_date,
    avg_duration_nano
  )
  VALUES (
    S.submission_date,
    FARM_FINGERPRINT(GENERATE_UUID()),
    S.trace_signature,
    S.hit_count,
    S.first_seen_date,
    S.latest_seen_date,
    S.avg_duration_nano
  )
WHEN MATCHED THEN
  UPDATE SET
    hit_count = T.hit_count + S.hit_count,
    latest_seen_date = GREATEST(T.latest_seen_date, S.latest_seen_date),
    avg_duration_nano = (T.avg_duration_nano * T.hit_count + S.avg_duration_nano * S.hit_count) / (T.hit_count + S.hit_count);
