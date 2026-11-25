-- Tasks before fxci_derived.tasks_v2 starts (2025-02-26), rebuilt from the legacy
-- taskclusteretl tables. Like tasks_v2, there is one row per task whose first
-- run (run 0) is resolved. The pulse_task messages give the task and the day,
-- the task definition gives the queue and the tags.
WITH first_runs AS (
  SELECT
    status.taskId AS task_id,
    DATE(TIME) AS submission_date
  FROM
    `moz-fx-data-shared-prod.taskclusteretl.pulse_task`
  WHERE
    DATE(TIME) = @submission_date
    AND DATE(TIME) < DATE "2025-02-26"
    AND runId = 0
    AND status.runs[SAFE_OFFSET(0)].state IN ("completed", "failed", "exception")
  GROUP BY
    task_id,
    submission_date
),
-- A task is created at most 5 days before its runs, because its deadline cannot
-- be further away than that, so look for its definition in the week before.
raw_definitions AS (
  SELECT
    taskId,
    schedulerId,
    taskGroupId,
    provisionerId,
    workerType,
    -- Same normalization as the fxci export (BigQueryHandler._normalize_tag): dashes
    -- and spaces become underscores, an underscore goes before each capital letter
    -- that is not the first character, then everything is lower case.
    ARRAY(
      SELECT AS STRUCT
        LOWER(
          IF(
            REGEXP_CONTAINS(k.cleaned, r"^[A-Z]"),
            SUBSTR(REGEXP_REPLACE(k.cleaned, r"([A-Z])", r"_\1"), 2),
            REGEXP_REPLACE(k.cleaned, r"([A-Z])", r"_\1")
          )
        ) AS name,
        k.value
      FROM
        (
          SELECT
            REPLACE(REPLACE(t.key, "-", "_"), " ", "_") AS cleaned,
            t.value
          FROM
            UNNEST(tags) AS t
        ) AS k
    ) AS normalized_tags
  FROM
    `moz-fx-data-shared-prod.taskclusteretl.task_definition`
  WHERE
    DATE(created)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND @submission_date
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY taskId ORDER BY created) = 1
),
definitions AS (
  SELECT
    taskId,
    schedulerId,
    taskGroupId,
    provisionerId,
    workerType,
    STRUCT(
      (
        SELECT
          MAX(IF(t.name = "created_for_user", t.value, NULL))
        FROM
          UNNEST(normalized_tags) AS t
      ) AS created_for_user,
      (SELECT MAX(IF(t.name = "kind", t.value, NULL)) FROM UNNEST(normalized_tags) AS t) AS kind,
      (SELECT MAX(IF(t.name = "label", t.value, NULL)) FROM UNNEST(normalized_tags) AS t) AS label,
      (SELECT MAX(IF(t.name = "os", t.value, NULL)) FROM UNNEST(normalized_tags) AS t) AS os,
      (
        SELECT
          MAX(IF(t.name = "owned_by", t.value, NULL))
        FROM
          UNNEST(normalized_tags) AS t
      ) AS owned_by,
      (
        SELECT
          MAX(IF(t.name = "project", t.value, NULL))
        FROM
          UNNEST(normalized_tags) AS t
      ) AS project,
      (
        SELECT
          MAX(IF(t.name = "trust_domain", t.value, NULL))
        FROM
          UNNEST(normalized_tags) AS t
      ) AS trust_domain,
      (
        SELECT
          MAX(IF(t.name = "worker_implementation", t.value, NULL))
        FROM
          UNNEST(normalized_tags) AS t
      ) AS worker_implementation,
      (
        SELECT
          MAX(IF(t.name = "test_suite", t.value, NULL))
        FROM
          UNNEST(normalized_tags) AS t
      ) AS test_suite,
      (
        SELECT
          MAX(IF(t.name = "test_platform", t.value, NULL))
        FROM
          UNNEST(normalized_tags) AS t
      ) AS test_platform,
      (
        SELECT
          MAX(IF(t.name = "test_variant", t.value, NULL))
        FROM
          UNNEST(normalized_tags) AS t
      ) AS test_variant
    ) AS tags
  FROM
    raw_definitions
)
SELECT
  definitions.schedulerId AS scheduler_id,
  first_runs.submission_date,
  definitions.taskGroupId AS task_group_id,
  first_runs.task_id,
  CONCAT(definitions.provisionerId, "/", definitions.workerType) AS task_queue_id,
  definitions.tags
FROM
  first_runs
INNER JOIN
  definitions
  ON first_runs.task_id = definitions.taskId
WHERE
  -- A task resolved just before the cutover can also have been ingested natively
  -- on the first days of tasks_v2.
  NOT EXISTS (
    SELECT
      1
    FROM
      `moz-fx-data-shared-prod.fxci_derived.tasks_v2` AS native
    WHERE
      native.submission_date
      BETWEEN DATE "2025-02-26"
      AND DATE "2025-02-28"
      AND native.task_id = first_runs.task_id
  )
