WITH ios_focus_unioned AS (
  SELECT
    *
  FROM
    org_mozilla_ios_focus.metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
-- no URI count on fx ios - there is cumulative tab count.
  SUM(CAST(NULL AS int64)) AS uri_count,
            -- no default browser setting on focus ios
  LOGICAL_OR(CAST(NULL AS boolean)) AS is_default_browser
FROM
  ios_focus_unioned
GROUP BY
  submission_date,
  client_id,
  sample_id
