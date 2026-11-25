-- Task definitions before fxci_derived.task_definitions_v1 starts (2025-10-07),
-- rebuilt from the legacy taskclusteretl.task_definition table. The legacy
-- table gets one row per finished run, so a retried task appears several times.
WITH deduplicated AS (
  SELECT
    taskId,
    created,
    deadline,
    dependencies,
    expires,
    extra,
    metadata,
    payload,
    priority,
    provisionerId,
    requires,
    retries,
    routes,
    schedulerId,
    scopes,
    tags,
    taskGroupId,
    workerType
  FROM
    `moz-fx-data-shared-prod.taskclusteretl.task_definition`
  WHERE
    DATE(created) = @submission_date
    AND DATE(created) < DATE "2025-10-07"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY taskId ORDER BY created) = 1
)
SELECT
  taskId,
  DATE(created) AS submission_date,
  created,
  deadline,
  dependencies,
  expires,
  -- extra and payload are JSON text in the legacy table. Keep the raw text as a
  -- JSON string rather than losing it when it does not parse.
  COALESCE(SAFE.PARSE_JSON(extra), TO_JSON(extra)) AS extra,
  TO_JSON(metadata) AS metadata,
  COALESCE(SAFE.PARSE_JSON(payload), TO_JSON(payload)) AS payload,
  priority,
  "none" AS projectId,
  provisionerId,
  requires,
  retries,
  routes,
  schedulerId,
  scopes,
  -- The tag keys and values are built the same way so they stay aligned, and
  -- ARRAY(SELECT ...) gives empty arrays, not NULL, for a task without tags.
  JSON_OBJECT(
    ARRAY(SELECT t.key FROM UNNEST(deduplicated.tags) AS t GROUP BY t.key ORDER BY t.key),
    ARRAY(
      SELECT
        ANY_VALUE(t.value)
      FROM
        UNNEST(deduplicated.tags) AS t
      GROUP BY
        t.key
      ORDER BY
        t.key
    )
  ) AS tags,
  taskGroupId,
  CONCAT(provisionerId, "/", workerType) AS taskQueueId,
  workerType
FROM
  deduplicated
WHERE
  -- A task created just before the cutover can also have been ingested natively
  -- on the first days of task_definitions_v1.
  NOT EXISTS (
    SELECT
      1
    FROM
      `moz-fx-data-shared-prod.fxci_derived.task_definitions_v1` AS native
    WHERE
      native.submission_date
      BETWEEN DATE "2025-10-07"
      AND DATE "2025-10-09"
      AND native.taskId = deduplicated.taskId
  )
