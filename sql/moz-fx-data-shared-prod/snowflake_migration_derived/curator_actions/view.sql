CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.snowflake_migration_derived.curator_actions`
AS
WITH curator_schedules AS (
  SELECT
    url,
    approved_corpus_item_external_id,
    scheduled_surface_id,
    topic,
    IFNULL(
      prospect_source,
      IF(corpus_item_loaded_from = 'MANUAL', corpus_item_loaded_from, prospect_source)
    ) AS prospect_source,
    'scheduled' AS STATUS,
    corpus_item_loaded_from AS loaded_from,
    DATE(reviewed_corpus_item_created_at) AS DATE_ADDED,
    scheduled_corpus_item_updated_at AS updated_at
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.scheduled_corpus_items`
),
curator_approvals AS (
  SELECT
    a.url,
    IFNULL(s.scheduled_surface_id, a.scheduled_surface_id) AS scheduled_surface_id,
    IFNULL(s.topic, a.topic) AS topic,
    a.prospect_id,
    COALESCE(s.prospect_source, a.prospect_source, a.loaded_from) AS prospect_source,
    IFNULL(s.status, a.corpus_review_status) AS STATUS,
    IFNULL(s.loaded_from, a.loaded_from) AS loaded_from,
    IFNULL(s.DATE_ADDED, DATE(a.reviewed_corpus_item_created_at)) AS DATE_ADDED,
    IFNULL(s.updated_at, a.reviewed_corpus_item_created_at) AS updated_at
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.approved_corpus_items` AS a
  LEFT JOIN
    curator_schedules AS s
    ON s.approved_corpus_item_external_id = a.approved_corpus_item_external_id
  WHERE
    a.loaded_from IN ('PROSPECT', 'MANUAL')
),
curator_rejects AS (
  SELECT
    url,
    scheduled_surface_id,
    topic,
    prospect_id,
    IFNULL(prospect_source, 'rejected') AS prospect_source,
    'rejected' AS STATUS, -- this is legacy label for rejected prospects to be consistent with backfill
    'REJECTED' AS loaded_from,
    DATE(reviewed_corpus_item_created_at) AS DATE_ADDED,
    reviewed_corpus_item_created_at AS updated_at
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.rejected_corpus_items_v1`
),
curator_dismissed AS (
  SELECT
    p.url,
    p.scheduled_surface_id,
    p.topic,
    p.prospect_id,
    IFNULL(p.prospect_source, p.prospect_review_status) AS prospect_source,
    p.prospect_review_status AS STATUS, -- this is legacy label for rejected prospects to be consistent with backfill
    'DISMISSED' AS loaded_from,
    DATE(p.created_at) AS DATE_ADDED,
    p.reviewed_at AS updated_at
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.dismissed_prospects_v1` AS p
),
merge_curator_decisions AS (
  SELECT
    *
  FROM
    curator_approvals
  UNION ALL
  SELECT
    *
  FROM
    curator_rejects
  UNION ALL
  SELECT
    *
  FROM
    curator_dismissed
)
SELECT
  *
FROM
  merge_curator_decisions;
