WITH date_range AS (
  -- Calculate the last 25 completed months
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 25 MONTH), MONTH) AS start_date,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) AS end_date
),
-- English articles reviewed on each day
en_articles_daily AS (
  SELECT
    DATE(lr.reviewed) AS review_date,
    d.id AS en_article_id
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document` d
  JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision` lr
    ON lr.id = d.latest_localizable_revision_id
  CROSS JOIN
    date_range dr
  WHERE
    d.locale = 'en-US'
    AND d.is_archived = FALSE
    AND d.is_localizable = TRUE
    AND d.latest_localizable_revision_id IS NOT NULL
    AND DATE(lr.reviewed) >= dr.start_date
    AND DATE(lr.reviewed) <= dr.end_date
)
SELECT
  review_date AS event_date,
  en_article_id,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  en_articles_daily
