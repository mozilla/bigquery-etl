CREATE TABLE IF NOT EXISTS
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_trace_events_v1`(
    trace_signature STRING,
    event_position INT64,
    event_signature STRING
  )
CLUSTER BY
  trace_signature;

MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_trace_events_v1` AS T
  USING (
    WITH RECURSIVE {% include '_shared/trace_chain_ctes.sql' %}
    SELECT
      tr.trace_signature,
      pe.event_position,
      pe.event_hash AS event_signature
    FROM
      trace_representatives tr
    JOIN
      path_events pe
      ON pe.trace_id = tr.instance.trace_id
      AND pe.leaf_span_id = tr.instance.leaf_span_id
  ) AS S
  ON T.trace_signature = S.trace_signature
  AND T.event_position = S.event_position
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (trace_signature, event_position, event_signature)
  VALUES
    (S.trace_signature, S.event_position, S.event_signature);
