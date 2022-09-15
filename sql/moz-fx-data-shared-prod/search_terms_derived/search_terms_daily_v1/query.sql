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
  date(timestamp) AS submission_date,
  query,
  count(*) AS search_sessions
FROM
  terminal_queries
GROUP BY
  1,
  2
-- Level 2 aggregation. See: https://wiki.mozilla.org/Data_Publishing
HAVING
  count(*) > 5000
