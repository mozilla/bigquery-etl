DECLARE submission_date DATE DEFAULT CURRENT_DATE() - 1;

SET submission_date = @submission_date;

WITH
{% include '_shared/trace_chain_ctes.sql' %}
-- All event signatures with their hashes, grouped by trace and event signature
trace_event_aggregates AS (
  SELECT
    submission_date,
    ts.trace_signature,
    ec.event_hash AS event_signature,
    COUNT(*) AS hit_count,
    MIN(ec.position) AS position
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
    tea.hit_count,
    tea.position
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
    hit_count,
    event_position
  )
  VALUES (
    S.submission_date,
    S.stable_trace_id,
    S.stable_event_id,
    S.hit_count,
    S.position
  )
WHEN MATCHED THEN
  UPDATE SET
    hit_count = T.hit_count + S.hit_count;
