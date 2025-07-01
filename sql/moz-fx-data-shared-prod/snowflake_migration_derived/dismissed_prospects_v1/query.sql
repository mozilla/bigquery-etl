SELECT
  p.authors,
  p.created_at,
  p.domain,
  p.excerpt,
  p.happened_at,
  p.image_url,
  p.is_collection,
  p.is_syndicated,
  p.language,
  p.prospect_id,
  p.prospect_review_status,
  p.prospect_source,
  p.publisher,
  p.reviewed_at,
  p.reviewed_by,
  p.scheduled_surface_id,
  p.status_reason_comment,
  p.status_reasons,
  p.title,
  p.topic,
  p.url
FROM
  `moz-fx-data-shared-prod.snowflake_migration_derived.prospects_v1` p
LEFT JOIN
  `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_updated_v1` AS c
  ON c.prospect_id = p.prospect_id
  {% if is_init() %}
    AND DATE(c.happened_at) >= '2024-09-19'
  {% else %}
    AND DATE(c.happened_at) = @submission_date
  {% endif %}
LEFT JOIN
  `moz-fx-data-shared-prod.snowflake_migration_derived.rejected_corpus_items_v1` AS r
  ON r.prospect_id = p.prospect_id
  {% if is_init() %}
    AND DATE(r.happened_at) >= '2024-09-19'
  {% else %}
    AND DATE(r.happened_at) = @submission_date
  {% endif %}
WHERE
  p.prospect_review_status = 'dismissed'
  {% if is_init() %}
    AND DATE(p.happened_at) >= '2024-09-19'
  {% else %}
    AND DATE(p.happened_at) = @submission_date
  {% endif %}
  AND c.approved_corpus_item_external_id IS NULL
  AND r.rejected_corpus_item_external_id IS NULL
