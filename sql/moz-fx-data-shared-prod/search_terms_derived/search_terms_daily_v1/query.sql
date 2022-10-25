WITH terminal_queries AS (
  SELECT
    array_reverse(ARRAY_AGG(sanitized ORDER BY sequence_no, timestamp ASC))[SAFE_OFFSET(0)].*
  FROM
    `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3` sanitized
  WHERE
    date(timestamp) = @submission_date
  GROUP BY
    sanitized.session_id
)
SELECT
  datetime_trunc(timestamp, week) AS submission_week,
  query,
  count(*) AS search_sessions
FROM
  terminal_queries
GROUP BY
  1,
  2
-- Threshold decision (not a public doc): https://docs.google.com/document/d/1xPiQP-72B4MtgdIZRbyYJcwlKh5pA9yLZ8gxC6rrwEs/edit#heading=h.ddfi2mqlhlyw
HAVING
  count(*) > 1
