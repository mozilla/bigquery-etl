WITH terminal_queries AS (
  SELECT
    ARRAY_REVERSE(ARRAY_AGG(sanitized ORDER BY sequence_no, timestamp ASC))[SAFE_OFFSET(0)].*
  FROM
    `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3` sanitized
  WHERE
    DATE(timestamp) > date_sub(@submission_date, INTERVAL 7 day)
  GROUP BY
    sanitized.session_id
),
daily_queries AS (
  SELECT
    DATE(timestamp) AS submission_date,
    query,
    COUNT(*) AS search_sessions
  FROM
    terminal_queries
  GROUP BY
    1,
    2
),
daily_queries_with_threshold AS (
  SELECT
    *,
    SUM(search_sessions) OVER (
      PARTITION BY
        QUERY
      ORDER BY
        SUBMISSION_DATE ASC
    ) AS search_sessions_in_last_7_days
  FROM
    DAILY_QUERIES
)
SELECT
  * EXCEPT (search_sessions_in_last_7_days)
FROM
  daily_queries_with_threshold
WHERE
  --Threshold decision (not a public doc): https://docs.google.com/document/d/1xPiQP-72B4MtgdIZRbyYJcwlKh5pA9yLZ8gxC6rrwEs/edit#heading=h.ddfi2mqlhlyw
  search_sessions_in_last_7_days > 1
  AND submission_date = @submission_date
