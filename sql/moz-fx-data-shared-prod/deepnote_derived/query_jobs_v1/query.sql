-- BigQuery query jobs that originate from Mozilla's default Deepnote workspace.
-- SQL blocks append a comment with Deepnote metadata to the query, e.g.
-- /* {"app":"deepnote","submitted_by":"scheduling","user_email":"...","workspace_id":"...",
--     "project_id":"...","notebook_id":"...","project_url":"..."} */
-- Queries sent from Python (code blocks, Streamlit apps) carry no comment and can
-- only be attributed to the Deepnote service account.
WITH jobs AS (
  SELECT
    *,
    SAFE.PARSE_JSON(
      REGEXP_EXTRACT(query, r'/\*\s*(\{"app"\s*:\s*"deepnote"[^*]*\})\s*\*/')
    ) AS deepnote_comment,
  FROM
    `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`
  WHERE
    DATE(creation_time) = @submission_date
    AND job_type = 'QUERY'
    -- script parent jobs aggregate the stats of their child jobs
    AND statement_type IS DISTINCT FROM 'SCRIPT'
    AND (
      user_email = 'external-deepnote@mozdata.iam.gserviceaccount.com'
      OR REGEXP_CONTAINS(query, r'"app"\s*:\s*"deepnote"')
    )
)
SELECT
  DATE(creation_time) AS submission_date,
  creation_time,
  start_time,
  end_time,
  job_id,
  project_id AS gcp_project_id,
  user_email AS principal_email,
  STARTS_WITH(user_email, 'external-deepnote') AS is_deepnote_service_account,
  CASE
    WHEN deepnote_comment IS NOT NULL
      THEN 'sql_block'
    WHEN STARTS_WITH(job_id, 'job_')
      AND REGEXP_CONTAINS(query, r'(?i)INFORMATION_SCHEMA')
      THEN 'schema_browser'
    ELSE 'python_client'
  END AS query_source,
  JSON_VALUE(deepnote_comment, '$.submitted_by') AS submitted_by,
  JSON_VALUE(deepnote_comment, '$.user_email') AS deepnote_user_email,
  JSON_VALUE(deepnote_comment, '$.workspace_id') AS deepnote_workspace_id,
  JSON_VALUE(deepnote_comment, '$.project_id') AS deepnote_project_id,
  JSON_VALUE(deepnote_comment, '$.notebook_id') AS deepnote_notebook_id,
  state,
  error_result.reason AS error_reason,
  cache_hit,
  statement_type,
  reservation_id,
  total_bytes_processed,
  total_bytes_billed,
  total_slot_ms,
  TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
  query_info.query_hashes.normalized_literals AS query_hash,
FROM
  jobs
WHERE
  -- uncommented service account queries are assumed to come from the default workspace
  IFNULL(
    JSON_VALUE(deepnote_comment, '$.workspace_id') = '5730346c-9297-42b0-80b0-35611206a1d7',
    TRUE
  )
