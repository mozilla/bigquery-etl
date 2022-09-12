WITH query_logs AS (
  SELECT
    DATE(timestamp) AS merino_date,
    jsonPayload.fields.*
  FROM
    `suggest-searches-prod-a30f.logs.stdout`
  WHERE
    jsonPayload.type = "web.suggest.request"
    AND jsonPayload.fields.session_id IS NOT NULL
    AND DATE(timestamp) = @submission_date
),
terminal_queries AS (
  SELECT
    array_agg(ql ORDER BY sequence_no DESC LIMIT 1)[offset(0)].*
  FROM
    query_logs ql
  GROUP BY
    ql.session_id
)
SELECT
  merino_date,
  query AS search_term,
  count(*) search_sessions
FROM
  terminal_queries
GROUP BY
  1,
  2
-- Level 2 aggregation. See: https://wiki.mozilla.org/Data_Publishing
HAVING
  count(*) > 5000
