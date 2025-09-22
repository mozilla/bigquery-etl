CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_current_v1`
AS
WITH corpus_item_id_row_nums AS (
  SELECT
    approved_corpus_item_external_id AS corpus_item_id,
    title,
    url AS recommendation_url,
    authors,
    publisher,
    reviewed_corpus_item_updated_at AS corpus_item_updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY
        approved_corpus_item_external_id
      ORDER BY
        reviewed_corpus_item_updated_at DESC
    ) AS row_num
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_updated_v1`
)
SELECT
  *
FROM
  corpus_item_id_row_nums
WHERE
  row_num = 1
