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

        -- Translations reviewed within 30 days of English version
        translation_revisions_daily AS (
            SELECT
                r.reviewed::DATE AS review_date,
                d.parent_id AS en_article_id,
                d.locale AS locale,
                d.id AS translation_doc_id
            FROM wiki_document d
            JOIN wiki_revision r ON r.document_id = d.id
            JOIN wiki_document parent ON parent.id = d.parent_id
            JOIN wiki_revision lr ON lr.id = parent.latest_localizable_revision_id
            CROSS JOIN date_range dr
            WHERE d.locale IN ('fr', 'es', 'de', 'it', 'zh-CN')
              AND d.current_revision_id IS NOT NULL
              -- Translation must be based on latest English version
              AND r.based_on_id >= parent.latest_localizable_revision_id
              -- Translation reviewed after English
              AND r.reviewed >= lr.reviewed
              -- Translation completed within 30-day SLA
              AND r.reviewed <= lr.reviewed + INTERVAL '30 days'
              -- Within date range
              AND r.reviewed::DATE >= dr.start_date
              AND r.reviewed::DATE <= dr.end_date
        )
        SELECT
            *
        FROM translation_revisions_daily
        """
    )
)
SELECT
  review_date AS date_key,
  locale,
  translation_doc_id,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  external_data
