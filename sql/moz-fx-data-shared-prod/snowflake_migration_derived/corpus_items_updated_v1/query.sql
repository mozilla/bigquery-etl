SELECT
  r.approved_corpus_item_external_id,
  r.prospect_id,
  r.url,
  r.loaded_from,
  r.corpus_review_status,
  CASE
    WHEN r.object_update_trigger = 'reviewed_corpus_item_added'
      THEN 'added'
    WHEN r.object_update_trigger = 'reviewed_corpus_item_updated'
      THEN 'updated'
    WHEN r.object_update_trigger = 'reviewed_corpus_item_removed'
      THEN 'removed'
  END AS reviewed_corpus_update_status,
  r.action_ui_page AS reviewed_action_ui_page,
  p.scheduled_surface_id,
  p.predicted_topic,
  p.prospect_source,
  p.flow AS prospect_flow,
  p.expires_at AS prospect_expires_at,
  r.title,
  r.excerpt,
  r.image_url,
  r.language,
  r.topic,
  r.authors,
  r.publisher,
  r.is_collection,
  r.is_syndicated,
  r.is_time_sensitive,
  r.reviewed_corpus_item_created_at,
  r.curator_created_by,
  r.reviewed_corpus_item_updated_at,
  r.curator_updated_by,
  r.happened_at
FROM
  `moz-fx-data-shared-prod.snowflake_migration_derived.stg_reviewed_corpus_items_v1` AS r
LEFT JOIN
  `moz-fx-data-shared-prod.snowflake_migration_derived.prospect_item_feed_v1` AS p
  ON p.prospect_id = r.prospect_id
WHERE
  r.object_version = 'new' --only select the new version from each reviewed_corpus_item event
  AND r.approved_corpus_item_external_id IS NOT NULL
  AND r.object_update_trigger IN (
    'reviewed_corpus_item_added',
    'reviewed_corpus_item_updated',
    'reviewed_corpus_item_removed'
  )
  {% if is_init() %}
    -- 2024-09-19 is the earliest date we have data from snowplow
    AND DATE(r.happened_at) >= '2024-09-19'
  {% else %}
    -- @submission_date is the default name for the query param
    -- automatically passed in when the job runs
    AND DATE(r.happened_at) = @submission_date
  {% endif %}
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      approved_corpus_item_external_id
    ORDER BY
      reviewed_corpus_item_updated_at DESC
  ) = 1
