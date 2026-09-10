CREATE TABLE IF NOT EXISTS
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1`(
    event_signature STRING,
    stable_event_id STRING,
    source_file STRING,
    source_line INT64,
    result STRING,
    first_seen_date DATE,
    last_seen_date DATE
  )
CLUSTER BY
  event_signature;

MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1` AS T
  USING (
    WITH {% include '_shared/event_ctes.sql' %},
    daily_events AS (
      SELECT
        event_hash AS event_signature,
        ANY_VALUE(source_file) AS source_file,
        ANY_VALUE(source_line) AS source_line,
        ANY_VALUE(result) AS result
      FROM
        span_event_hashes
      GROUP BY
        event_hash
    )
    SELECT
      *
    FROM
      daily_events
  ) AS S
  ON T.event_signature = S.event_signature
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (event_signature, stable_event_id, source_file, source_line, result, first_seen_date, last_seen_date)
  VALUES
    (
      S.event_signature,
      GENERATE_UUID(),
      S.source_file,
      S.source_line,
      S.result,
      @submission_date,
      @submission_date
    )
  WHEN MATCHED
THEN
  UPDATE
  SET
    first_seen_date = LEAST(T.first_seen_date, @submission_date),
    last_seen_date = GREATEST(T.last_seen_date, @submission_date);
