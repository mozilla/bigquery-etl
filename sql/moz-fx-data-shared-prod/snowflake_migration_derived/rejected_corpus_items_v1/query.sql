SELECT
  r.authors,
  r.corpus_review_status,
  r.curator_created_by,
  r.curator_updated_by,
  r.excerpt,
  r.happened_at,
  r.image_url,
  r.is_collection,
  r.is_syndicated,
  r.is_time_sensitive,
  r.language,
  r.loaded_from,
  p.predicted_topic,
  p.expires_at AS prospect_expires_at,
  p.flow AS prospect_flow,
  r.prospect_id,
  p.prospect_source,
  r.publisher,
  r.rejected_corpus_item_external_id,
  r.action_ui_page AS rejection_action_ui_page,
  r.rejection_reasons,
  r.reviewed_corpus_item_created_at,
  r.reviewed_corpus_item_updated_at,
  p.scheduled_surface_id,
  r.title,
  r.topic,
  r.url,
FROM
  `moz-fx-data-shared-prod.snowflake_migration_derived.stg_reviewed_corpus_items_v1` AS r
LEFT JOIN
  `moz-fx-data-shared-prod.snowflake_migration_derived.prospect_item_feed_v1` AS p
  ON p.prospect_id = r.prospect_id
WHERE
  object_version = 'new' --only select the new version from each reviewed_corpus_item event
  AND r.rejected_corpus_item_external_id IS NOT NULL
  {% if is_init() %}
    -- 2024-09-19 is the earliest date we have data from snowplow
    AND DATE(r.happened_at) >= '2024-09-19'
    AND DATE(p.happened_at) >= '2024-09-19'
  {% else %}
    -- @submission_date is the default name for the query param
    -- automatically passed in when the job runs
    AND DATE(r.happened_at) = @submission_date
    AND DATE(p.happened_at) = @submission_date
  {% endif %}
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      r.rejected_corpus_item_external_id
    ORDER BY
      r.happened_at DESC
  ) = 1
