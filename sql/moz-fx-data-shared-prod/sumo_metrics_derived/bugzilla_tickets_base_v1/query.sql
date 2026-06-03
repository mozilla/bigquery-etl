WITH bug_history AS (
  SELECT
    id,
    DATE(creation_ts) AS creation_date,
    MIN(
      COALESCE(
        IF(status = 'RESOLVED', snapshot_date, NULL),
        IF(resolution IS NOT NULL, snapshot_date, NULL)
      )
    ) AS first_resolved_date,
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
),
-- Point-in-time count of bugs open as of @submission_date: created on or before
-- that day and not yet resolved as of that day, regardless of creation date.
open_bugs AS (
  SELECT
    COUNT(*) AS bugs_open,
  FROM
    bugs
  WHERE
    creation_date <= @submission_date
    AND (resolution_date IS NULL OR resolution_date > @submission_date)
)
SELECT
  @submission_date AS `date`,
  COALESCE(created.bugs_created, 0) AS bugs_created,
  COALESCE(resolved.bugs_resolved, 0) AS bugs_resolved,
  open_bugs.bugs_open,
  CURRENT_TIMESTAMP() AS etl_timestamp,
FROM
  open_bugs
LEFT JOIN
  created
  ON created.date = @submission_date
LEFT JOIN
  resolved
  ON resolved.date = @submission_date
