SELECT
  s.approved_corpus_item_external_id,
  a.authors,
  TO_BASE64(
    SHA256(
      CONCAT(
        s.approved_corpus_item_external_id,
        s.scheduled_surface_id,
        s.scheduled_corpus_item_scheduled_at
      )
    )
  ) AS corpus_item_id_scheduled_surface_id_scheduled_at_key,
  a.loaded_from AS corpus_item_loaded_from,
  a.corpus_review_status,
  s.curator_created_by,
  s.curator_updated_by,
  a.excerpt,
  s.happened_at,
  a.image_url,
  a.is_collection,
  a.is_syndicated,
  a.is_time_sensitive,
  a.language,
  a.predicted_topic,
  a.prospect_expires_at,
  a.prospect_flow,
  a.prospect_id,
  a.scheduled_surface_id AS prospect_scheduled_surface_id,
  a.prospect_source,
  a.publisher,
  a.reviewed_corpus_item_created_at,
  a.reviewed_corpus_item_updated_at,
  a.reviewed_corpus_update_status,
  s.scheduled_action_ui_page,
  s.scheduled_corpus_item_created_at,
  s.scheduled_corpus_item_external_id,
  s.scheduled_corpus_item_scheduled_at,
  s.scheduled_corpus_item_updated_at,
  CASE
    WHEN s.object_update_trigger = 'scheduled_corpus_item_added'
      THEN 'added'
    WHEN s.object_update_trigger = 'scheduled_corpus_item_removed'
      THEN 'removed'
    WHEN s.object_update_trigger = 'scheduled_corpus_item_rescheduled'
      THEN 'rescheduled'
  END AS scheduled_corpus_status,
  s.scheduled_status,
  s.scheduled_status_reasons,
  s.scheduled_status_reason_comment,
  s.scheduled_surface_iana_timezone,
  s.scheduled_surface_id,
  s.scheduled_surface_name,
  IFNULL(s.schedule_generated_by, 'MANUAL') AS schedule_generated_by,
  a.title,
  a.topic,
  s.url
FROM
  `moz-fx-data-shared-prod.snowflake_migration_derived.stg_scheduled_corpus_items_v1` s
JOIN
  `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_updated_v1` AS a
  ON a.approved_corpus_item_external_id = s.approved_corpus_item_external_id
WHERE
  object_version = 'new' --only select the new version from each scheduled_corpus_item event
  AND s.object_update_trigger IN (
    'scheduled_corpus_item_added',
    'scheduled_corpus_item_removed',
    'scheduled_corpus_item_rescheduled'
  )
  {% if is_init() %}
    -- 2024-09-19 is the earliest date we have data from snowplow
    AND DATE(s.happened_at) >= '2024-09-19'
  {% else %}
    -- @submission_date is the default name for the query param
    -- automatically passed in when the job runs
    AND DATE(s.happened_at) = @submission_date
  {% endif %}
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      s.approved_corpus_item_external_id,
      s.scheduled_surface_id,
      s.scheduled_corpus_item_scheduled_at
    ORDER BY
      s.scheduled_corpus_item_updated_at DESC
  ) = 1
