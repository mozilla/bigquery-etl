CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_content_items_daily_combined_v1`
AS
SELECT
  content.*,
  corpus_items.* EXCEPT (corpus_item_id, row_num)
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_content_items_daily_v1` AS content
LEFT OUTER JOIN
  `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_current_v1` AS corpus_items
  ON content.corpus_item_id = corpus_items.corpus_item_id
