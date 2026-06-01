WITH bug_history AS (
  SELECT
    id,
    DATE(creation_ts) AS creation_date,
    MIN(IF(status = 'RESOLVED', snapshot_date, NULL)) AS first_resolved_date,
    MAX(IF(status = 'REOPENED', snapshot_date, NULL)) AS last_reopened_resolved_date,
  FROM
    `moz-fx-data-shared-prod.bugzilla_metrics.bugs`
  WHERE
    component = 'Knowledge Base Content'
  GROUP BY
    id,
    creation_date
),
bugs AS (
  SELECT
    id,
    creation_date,
    GREATEST(
      first_resolved_date,
      COALESCE(last_reopened_resolved_date, DATE '1900-01-01')
    ) AS resolution_date,
  FROM
    bug_history
),
created AS (
  SELECT
    creation_date AS `date`,
    COUNT(*) AS bugs_created,
  FROM
    bugs
  WHERE
    creation_date = @submission_date
  GROUP BY
    `date`
),
resolved AS (
  SELECT
    resolution_date AS `date`,
    COUNT(*) AS bugs_resolved,
  FROM
    bugs
  WHERE
    resolution_date = @submission_date
  GROUP BY
    `date`
)
SELECT
  @submission_date AS `date`,
  COALESCE(created.bugs_created, 0) AS bugs_created,
  COALESCE(resolved.bugs_resolved, 0) AS bugs_resolved,
  CURRENT_TIMESTAMP() AS etl_timestamp,
FROM
  created
FULL OUTER JOIN
  resolved
  USING (`date`)
