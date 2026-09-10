CREATE TABLE IF NOT EXISTS
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1`(
    trace_signature STRING,
    stable_trace_id STRING,
    trace_key STRING,
    first_seen_date DATE,
    last_seen_date DATE
  )
CLUSTER BY
  trace_signature;

MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1` AS T
  USING (
    WITH RECURSIVE {% include '_shared/trace_chain_ctes.sql' %},
    -- Traces whose events are not yet in gecko_trace_events_v1 are skipped
    -- so that a partial event list never produces a wrong trace_key.
    trace_keys AS (
      SELECT
        tr.trace_signature,
        TO_BASE64(SHA256(STRING_AGG(ev.stable_event_id, ',' ORDER BY pe.event_position))) AS trace_key
      FROM
        trace_representatives tr
      JOIN
        path_events pe
        ON pe.trace_id = tr.instance.trace_id
        AND pe.leaf_span_id = tr.instance.leaf_span_id
      LEFT JOIN
        `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1` ev
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
        `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1`
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
