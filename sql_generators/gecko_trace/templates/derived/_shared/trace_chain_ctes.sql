{#- Leaf-to-root span walk, ordered events, trace signatures and one representative
    instance per signature. Requires WITH RECURSIVE. Callers append CTEs after a comma. -#}
{% include '_shared/event_ctes.sql' %},
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
)
