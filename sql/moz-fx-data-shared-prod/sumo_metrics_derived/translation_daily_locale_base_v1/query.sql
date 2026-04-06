WITH date_range AS (
  -- Calculate the last 25 completed months
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 25 MONTH), MONTH) AS start_date,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) AS end_date
),
-- Translations reviewed within 30 days of English version
translation_revisions_daily AS (
  SELECT
    DATE(r.reviewed) AS review_date,
    d.parent_id AS en_article_id,
    d.locale AS locale,
    d.id AS translation_doc_id
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document` d
  JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision` r
    ON r.document_id = d.id
  JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document` parent
    ON parent.id = d.parent_id
  JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision` lr
    ON lr.id = parent.latest_localizable_revision_id
  CROSS JOIN
    date_range dr
  WHERE
    d.locale IN ('fr', 'es', 'de', 'it', 'zh-CN')
    AND d.current_revision_id IS NOT NULL
    -- Translation must be based on latest English version
    AND r.based_on_id >= parent.latest_localizable_revision_id
    -- Translation reviewed after English
    AND r.reviewed >= lr.reviewed
    -- Translation completed within 30-day SLA
    AND r.reviewed <= lr.reviewed + INTERVAL 30 DAY
    -- Within date range
    AND DATE(r.reviewed) >= dr.start_date
    AND DATE(r.reviewed) <= dr.end_date
)
SELECT
  review_date AS event_date,
  locale,
  translation_doc_id,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  translation_revisions_daily
