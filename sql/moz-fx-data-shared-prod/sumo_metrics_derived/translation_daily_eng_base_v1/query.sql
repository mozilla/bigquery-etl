WITH external_data AS (
  SELECT
    *
  FROM
    EXTERNAL_QUERY(
      "projects/moz-fx-sumo-prod/locations/us/connections/sumo-prod-prod-db",
      """
        WITH date_range AS (
            -- Calculate the last 13 completed months
            -- Since today is April 10, 2025, the last completed month is March 2025
            -- We want to go back 13 months, so we start from March 2024
            SELECT
                DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month' - INTERVAL '24 months')::DATE AS start_date,
                (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS end_date
        ),
        -- English articles reviewed on each day
        en_articles_daily AS (
            SELECT
                lr.reviewed::DATE AS review_date,
                d.id AS en_article_id
            FROM wiki_document d
            JOIN wiki_revision lr ON lr.id = d.latest_localizable_revision_id
            CROSS JOIN date_range dr
            WHERE d.locale = 'en-US'
              AND d.is_archived = FALSE
              AND d.is_localizable = TRUE
              AND d.latest_localizable_revision_id IS NOT NULL
              AND lr.reviewed::DATE >= dr.start_date
              AND lr.reviewed::DATE <= dr.end_date
        )
        SELECT
            *
        FROM en_articles_daily

        """
    )
)
SELECT
  review_date AS event_date,
  en_article_id,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  external_data
