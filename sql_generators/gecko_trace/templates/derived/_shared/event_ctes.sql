{#- Raw span extraction and per-event hashing. Callers append their own CTEs after a comma. -#}
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
    `{{ target_project }}.{{ app_id }}_stable.{{ ping_name }}_v1`
  CROSS JOIN
    UNNEST(
      JSON_QUERY_ARRAY(metrics.object.gecko_trace_traces_data, '$.resource_spans')
    ) AS resource_span
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
    COALESCE(JSON_VALUE(e, '$.attributes."source.file"'), '') AS source_file,
    SAFE_CAST(JSON_VALUE(e, '$.attributes."source.line"') AS INT64) AS source_line,
    COALESCE(JSON_VALUE(e, '$.attributes.result'), '') AS result,
    TO_BASE64(
      SHA256(
        CONCAT(
          COALESCE(JSON_VALUE(e, '$.attributes."source.file"'), ''),
          '\x00',
          COALESCE(JSON_VALUE(e, '$.attributes."source.line"'), ''),
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
)
