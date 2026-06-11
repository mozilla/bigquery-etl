DECLARE submission_date DATE DEFAULT CURRENT_DATE() - 1;

SET submission_date = @submission_date;

-- Raw spans: expand glean pings
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
-- Flag spans with events
span_has_events AS (
  SELECT
    rs.trace_id,
    rs.span_id,
    ARRAY_LENGTH(rs.events) > 0 AS has_events
  FROM
    raw_spans rs
),
-- Effective parent (same as traces script)
span_effective_parent AS (
  SELECT
    rs.trace_id,
    rs.span_id,
    CASE
      WHEN rs.parent_span_id IS NULL THEN NULL
      WHEN sha.has_events THEN rs.parent_span_id
      ELSE NULL
    END AS effective_parent_span_id,
    1 AS depth
  FROM
    raw_spans rs
  LEFT JOIN
    span_has_events sha ON rs.parent_span_id = sha.span_id AND rs.trace_id = sha.trace_id
  WHERE
    rs.parent_span_id IS NULL OR sha.has_events
  UNION ALL
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
-- Per-event hashes
event_hash_rows AS (
  SELECT
    rs.trace_id,
    rs.span_id,
    rs.parent_span_id,
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
-- Event hashes with submission_date for grouping
event_hash_with_date AS (
  SELECT
    submission_date,
    ehr.*
  FROM
    event_hash_rows ehr
),
-- Event chain (same as traces script, with submission_date carried through)
event_chain AS (
  SELECT
    ehd.submission_date,
    ehd.trace_id,
    ehd.span_id,
    0 AS parent_event_offset,
    ehd.event_offset,
    ehd.span_event_count,
    ehd.event_hash,
    SHA256(ehd.event_hash) AS chain_sig
  FROM
    event_hash_with_date ehd
  LEFT JOIN
    span_effective_parent sep ON ehd.span_id = sep.span_id AND ehd.trace_id = sep.trace_id
  WHERE
    ehd.event_offset = 1 AND sep.effective_parent_span_id IS NULL
  UNION ALL
  SELECT
    ehd.submission_date,
    ehd.trace_id,
    ehd.span_id,
    parent_ec.event_offset AS parent_event_offset,
    ehd.event_offset,
    ehd.span_event_count,
    ehd.event_hash,
    SHA256(
      CONCAT(parent_ec.chain_sig, b'\x00', ehd.event_hash)
    ) AS chain_sig
  FROM
    event_hash_with_date ehd
  JOIN
    span_effective_parent sep ON ehd.span_id = sep.span_id AND ehd.trace_id = sep.trace_id
  JOIN
    event_chain parent_ec ON sep.effective_parent_span_id = parent_ec.span_id
      AND ehd.trace_id = parent_ec.trace_id
      AND parent_ec.event_offset = parent_ec.span_event_count
  WHERE
    ehd.event_offset = 1 AND sep.effective_parent_span_id IS NOT NULL
  UNION ALL
  SELECT
    ehd.submission_date,
    ehd.trace_id,
    ehd.span_id,
    prev_ec.event_offset AS parent_event_offset,
    ehd.event_offset,
    ehd.span_event_count,
    ehd.event_hash,
    SHA256(
      CONCAT(prev_ec.chain_sig, b'\x00', ehd.event_hash)
    ) AS chain_sig
  FROM
    event_hash_with_date ehd
  JOIN
    event_chain prev_ec ON ehd.span_id = prev_ec.span_id
      AND ehd.trace_id = prev_ec.trace_id
      AND ehd.event_offset = prev_ec.event_offset + 1
)
OPTIONS (max_iterations = 1000),
-- Leaf sigs
leaf_sigs AS (
  SELECT
    ec.trace_id,
    ARRAY_AGG(ec.chain_sig ORDER BY ec.chain_sig) AS leaf_chain_sigs
  FROM
    event_chain ec
  LEFT JOIN
    raw_spans child ON ec.span_id = child.parent_span_id AND ec.trace_id = child.trace_id
  WHERE
    ec.event_offset = ec.span_event_count
    AND child.span_id IS NULL
  GROUP BY
    ec.trace_id
),
-- Trace signature
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
-- All event signatures with their hashes, grouped by trace and event signature
trace_event_aggregates AS (
  SELECT
    submission_date,
    ts.trace_signature,
    ec.event_hash AS event_signature,
    COUNT(*) AS hit_count
  FROM
    trace_signature_cte ts
  JOIN
    event_chain ec ON ts.trace_id = ec.trace_id
  GROUP BY
    submission_date,
    ts.trace_signature,
    ec.event_hash
),
-- Join signatures to stable IDs
with_stable_ids AS (
  SELECT
    tea.submission_date,
    t.stable_trace_id,
    e.stable_event_id,
    tea.hit_count
  FROM
    trace_event_aggregates tea
  JOIN
    `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1` t ON tea.trace_signature = t.trace_signature
  JOIN
    `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1` e ON tea.event_signature = e.event_signature
)
-- MERGE into bridge table
MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_trace_events_v1` AS T
USING
  with_stable_ids AS S
ON
  T.stable_trace_id = S.stable_trace_id
  AND T.stable_event_id = S.stable_event_id
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    submission_date,
    stable_trace_id,
    stable_event_id,
    hit_count
  )
  VALUES (
    S.submission_date,
    S.stable_trace_id,
    S.stable_event_id,
    S.hit_count
  )
WHEN MATCHED THEN
  UPDATE SET
    hit_count = T.hit_count + S.hit_count;
