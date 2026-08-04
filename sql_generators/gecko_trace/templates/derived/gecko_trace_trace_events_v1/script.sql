CREATE TABLE IF NOT EXISTS
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_trace_events_v1` (
    submission_date DATE,
    stable_trace_id STRING,
    stable_event_id STRING,
    hit_count INT64,
    event_position INT64
  )
  CLUSTER BY stable_trace_id, stable_event_id;

MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_trace_events_v1` AS T
  USING (
    WITH RECURSIVE
{% include '_shared/trace_chain_ctes.sql' %}
new_traces AS (
  SELECT
    ts.trace_id,
    ts.leaf_span_id,
    ts.trace_signature,
    t.stable_trace_id
  FROM
    trace_signatures ts
  JOIN
    `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1` t
    ON ts.trace_signature = t.trace_signature
  LEFT JOIN
    `{{ target_project }}.{{ app_id }}_derived.gecko_trace_trace_events_v1` te
    ON t.stable_trace_id = te.stable_trace_id
  WHERE
    te.stable_trace_id IS NULL
),
new_trace_events AS (
  SELECT
    @submission_date AS submission_date,
    nt.stable_trace_id,
    ev.stable_event_id,
    COUNT(*) AS hit_count,
    MIN(pe.event_position) AS event_position
  FROM
    path_events pe
  JOIN
    new_traces nt
    ON pe.trace_id = nt.trace_id
    AND pe.leaf_span_id = nt.leaf_span_id
  JOIN
    `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1` ev
    ON pe.event_hash = ev.event_signature
  GROUP BY
    nt.stable_trace_id,
    ev.stable_event_id
)
    SELECT
      *
    FROM
      new_trace_events
  ) AS S
  ON T.stable_trace_id = S.stable_trace_id
  AND T.stable_event_id = S.stable_event_id
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    submission_date,
    stable_trace_id,
    stable_event_id,
    hit_count,
    event_position
  )
  VALUES (
    S.submission_date,
    S.stable_trace_id,
    S.stable_event_id,
    S.hit_count,
    S.event_position
  );
