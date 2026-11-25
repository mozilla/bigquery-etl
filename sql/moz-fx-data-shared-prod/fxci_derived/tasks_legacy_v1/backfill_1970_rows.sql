-- One-off, not scheduled. Run once after the daily backfill of tasks_legacy_v1
-- has covered 2020-08-01 to 2020-09-30, and after the one-off of
-- task_runs_legacy_v1 (same reason).
--
-- About a million tasks of 2020-08 and 2020-09 have their first resolved run in
-- the 1970-01-01 partition of taskclusteretl.pulse_task, because their message
-- has no event time. The daily backfill reads one partition per day, so it cannot
-- place them. This inserts them into their real partitions, dated from the run
-- itself, with the same joins as query.sql. Tasks already loaded from another
-- message are skipped.
INSERT INTO
  `moz-fx-data-shared-prod.fxci_derived.tasks_legacy_v1`(
    scheduler_id,
    submission_date,
    task_group_id,
    task_id,
    task_queue_id,
    tags
  )
WITH first_runs AS (
  SELECT
    status.taskId AS task_id,
    DATE(
      COALESCE(status.runs[SAFE_OFFSET(0)].started, status.runs[SAFE_OFFSET(0)].scheduled)
    ) AS submission_date
  FROM
    `moz-fx-data-shared-prod.taskclusteretl.pulse_task`
  WHERE
    DATE(TIME) = DATE "1970-01-01"
    AND runId = 0
    AND status.runs[SAFE_OFFSET(0)].state IN ("completed", "failed", "exception")
  GROUP BY
    task_id,
    submission_date
),
-- The definitions of tasks that ran in 2020-08 and 2020-09 were created at most
-- 5 days before, so from the end of July.
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
    BETWEEN DATE "2020-07-01"
    AND DATE "2020-09-30"
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
  NOT EXISTS (
    SELECT
      1
    FROM
      `moz-fx-data-shared-prod.fxci_derived.tasks_legacy_v1` AS loaded
    WHERE
      loaded.submission_date
      BETWEEN DATE "2020-07-01"
      AND DATE "2020-10-31"
      AND loaded.task_id = first_runs.task_id
  )
