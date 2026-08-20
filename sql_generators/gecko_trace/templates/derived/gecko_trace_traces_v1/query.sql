WITH spans AS (
  SELECT
    submission_timestamp,
    trace_id,
    span_id,
    parent_span_id,
    span_name AS name,
    start_time_unix_nano,
    end_time_unix_nano,
    events,
    resource,
    scope
  FROM
	`{{ target_project }}.{{ app_id }}_derived.gecko_trace_spans_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
trace AS (
  SELECT
    MIN(submission_timestamp) AS submission_timestamp,
    trace_id,
    MAX(end_time_unix_nano) - MIN(start_time_unix_nano) AS duration_nano,
    mozfun.gecko_trace.build_root_span(
      ARRAY_AGG(
        TO_JSON(
          STRUCT(
            span_id,
            parent_span_id,
            name,
            start_time_unix_nano,
            end_time_unix_nano,
            events,
            resource,
            scope
          )
        )
      )
    ) AS root_span
  FROM
    spans
  GROUP BY
    trace_id
)
SELECT
  submission_timestamp,
  mozfun.gecko_trace.calculate_signature(root_span) AS signature,
  trace_id,
  duration_nano,
  root_span
FROM
  trace
