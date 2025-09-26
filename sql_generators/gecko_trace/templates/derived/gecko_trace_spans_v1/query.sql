WITH ping AS (
  SELECT
    submission_timestamp,
    JSON_QUERY_ARRAY(metrics.object.gecko_trace_traces_data, "$.resource_spans") AS resource_spans
  FROM
    `{{ target_project }}.{{ app_id }}.{{ ping_name }}`
  WHERE
    DATE(submission_timestamp) = @submission_date
)
SELECT
  JSON_VALUE(span, "$.trace_id") AS trace_id,
  JSON_VALUE(span, '$.span_id') AS span_id,
  JSON_VALUE(span, '$.parent_span_id') AS parent_span_id,
  JSON_VALUE(span, '$.name') AS span_name,
  SAFE_CAST(JSON_VALUE(span, '$.start_time_unix_nano') AS INT64) AS start_time_unix_nano,
  SAFE_CAST(JSON_VALUE(span, '$.end_time_unix_nano') AS INT64) AS end_time_unix_nano,
  JSON_QUERY_ARRAY(span, '$.events') AS events,
  JSON_QUERY(resource_span, '$.resource') AS resource,
  JSON_QUERY(scope_span, '$.scope') AS scope
FROM
  ping
CROSS JOIN
  UNNEST(resource_spans) AS resource_span,
  UNNEST(JSON_QUERY_ARRAY(resource_span, '$.scope_spans')) AS scope_span,
  UNNEST(JSON_QUERY_ARRAY(scope_span, '$.spans')) AS span;
