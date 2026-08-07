{% set llm_model = 'gemini-2.5-pro' %}
{% set embedding_model = 'gemini-embedding-001' %}
{% set prompt_version = 'v1' %}
{% set category_threshold = 30 %}
{%- set article_prompt =
  'You are analyzing a Mozilla Knowledge Base support article. ' ~
  'Extract the following fields and return them as structured data. ' ~
  'article_summary_llm: maximum 8 words, clear, factual, in English. ' ~
  'article_category_llm: exactly 1 reusable classification label, preferably 1 word, maximum 2 words. ' ~
  'article_entities_llm: array of up to 3 important normalized entities (e.g. product names, features, error codes), short, deduplicated. ' ~
  'article_topics_llm: array of up to 3 normalized topics, preferably 1 word, maximum 2 words each, reusable across similar texts, suitable as classification labels. '
-%}
WITH
-- Latest approved-revision date per document. Mirrors the freshness signal in
-- `sumo_metrics_derived.freshness_metrics_base_v1` so the view's `is_stale`
-- flag aligns with the canonical CX freshness definition.
revision_freshness AS (
  SELECT
    document_id,
    MAX(DATE(reviewed)) AS last_approved_revision_date
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision`
  WHERE
    is_approved = TRUE
  GROUP BY
    document_id
),
base AS (
  SELECT
    d.id,
    d.title,
    d.slug,
    d.is_template,
    d.is_localizable,
    d.locale,
    d.html AS content,
    d.category,
    d.allow_discussion,
    d.needs_change,
    d.needs_change_comment,
    d.share_link,
    d.display_order,
    d.current_revision_id,
    d.latest_localizable_revision_id,
    d.parent_id,
    d.products,
    d.topics,
    d.last_updated,
    rf.last_approved_revision_date,
    d.num_pageviews_last_7_days,
    d.num_pageviews_last_30_days,
    d.num_pageviews_last_90_days,
    d.num_pageviews_last_365_days
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document_plus` d
  LEFT JOIN
    revision_freshness rf
    ON rf.document_id = d.id
  WHERE
    d.parent_id IS NULL -- original non-translated articles
    AND d.is_archived = FALSE
    AND d.category < {{ category_threshold }} -- Only categories relevant to the users. See schema description for further details.
    -- categories https://github.com/mozilla/kitsune/blob/3ddd61a2f32eb486388366874d42f9a860e357d8/kitsune/wiki/config.py#L87
    AND d.html NOT LIKE '%REDIRECT%'
    AND LOWER(d.products) NOT LIKE '%thunderbird%'
),
existing AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.customer_experience_derived.knowledge_base_retrieval_index_v1`
),
-- Carry forward rows whose content (revision) is unchanged AND whose prior AI
-- run succeeded AND matches the current model/prompt versions. These rows skip
-- all AI.GENERATE / AI.EMBED calls. `last_approved_revision_date` is always
-- recomputed from `revision_freshness` (via `base`) — a new approved revision
-- can land in `kitsune_wiki_revision` without bumping `current_revision_id`,
-- so the freshness signal must not be carried forward as-is.
unchanged AS (
  SELECT
    e.* REPLACE (b.last_approved_revision_date AS last_approved_revision_date)
  FROM
    existing e
  JOIN
    base b
    USING (id)
  WHERE
    e.current_revision_id = b.current_revision_id
    AND e.metadata.embedding_succeeded
    AND e.metadata.embedding_version = '{{ embedding_model }}'
    AND e.metadata.model_version = '{{ llm_model }}'
    AND e.metadata.prompt_version = '{{ prompt_version }}'
),
-- Rows that must be (re)processed: new ids, revision bumps, prior embedding
-- failures, or changes to the LLM model, embedding model, or prompt version.
-- Deleted-upstream rows are dropped naturally because neither `unchanged` nor
-- `needs_processing` retains them.
needs_processing AS (
  SELECT
    b.*
  FROM
    base b
  LEFT JOIN
    unchanged un
    USING (id)
  WHERE
    un.id IS NULL
),
articles_llm AS (
  SELECT
    id,
    AI.GENERATE(
      prompt => CONCAT('{{ article_prompt }}', '\n', 'Title: ', title, '\n', 'Content: ', content),
      endpoint => '{{ llm_model }}',
      output_schema => 'article_summary_llm STRING, article_category_llm STRING, article_entities_llm ARRAY<STRING>, article_topics_llm ARRAY<STRING>'
    ) AS llm_result
  FROM
    needs_processing
),
articles_embedding AS (
  SELECT
    id,
    AI.EMBED(CONCAT(title, ' ', content), endpoint => '{{ embedding_model }}').result AS embedding
  FROM
    needs_processing
),
newly_processed AS (
  SELECT
    id,
    title,
    slug,
    locale,
    content,
    category,
    needs_change,
    needs_change_comment,
    share_link,
    display_order,
    current_revision_id,
    latest_localizable_revision_id,
    parent_id,
    products,
    topics,
    is_template,
    is_localizable,
    allow_discussion,
    last_updated,
    last_approved_revision_date,
    num_pageviews_last_7_days,
    num_pageviews_last_30_days,
    num_pageviews_last_90_days,
    num_pageviews_last_365_days,
    'article' AS type,
    articles_llm.llm_result.article_summary_llm,
    articles_llm.llm_result.article_category_llm,
    articles_llm.llm_result.article_entities_llm,
    articles_llm.llm_result.article_topics_llm,
    embedding,
    STRUCT(
      ['title', 'content'] AS input_fields,
      LENGTH(CONCAT(needs_processing.title, ' ', needs_processing.content)) AS input_char_count,
      '{{ llm_model }}' AS model_version,
      '{{ embedding_model }}' AS embedding_version,
      '{{ prompt_version }}' AS prompt_version,
      CURRENT_TIMESTAMP() AS analysis_timestamp,
      embedding IS NOT NULL AS embedding_succeeded,
      ARRAY_CONCAT(
        IF(embedding IS NULL, ['embedding_missing'], []),
        IF(
          articles_llm.llm_result.article_summary_llm IS NULL
          OR LENGTH(TRIM(articles_llm.llm_result.article_summary_llm)) = 0,
          ['article_summary_llm_missing'],
          []
        ),
        IF(
          articles_llm.llm_result.article_category_llm IS NULL
          OR LENGTH(TRIM(articles_llm.llm_result.article_category_llm)) = 0,
          ['article_category_llm_missing'],
          []
        ),
        IF(
          articles_llm.llm_result.article_entities_llm IS NULL
          OR ARRAY_LENGTH(articles_llm.llm_result.article_entities_llm) = 0,
          ['article_entities_llm_missing'],
          []
        ),
        IF(
          EXISTS (
            SELECT
              1
            FROM
              UNNEST(articles_llm.llm_result.article_entities_llm) t
            WHERE
              t IS NULL
              OR LENGTH(TRIM(t)) = 0
          ),
          ['article_entities_llm_has_empty_elements'],
          []
        ),
        IF(
          articles_llm.llm_result.article_topics_llm IS NULL
          OR ARRAY_LENGTH(articles_llm.llm_result.article_topics_llm) = 0,
          ['article_topics_llm_missing'],
          []
        ),
        IF(
          EXISTS (
            SELECT
              1
            FROM
              UNNEST(articles_llm.llm_result.article_topics_llm) t
            WHERE
              t IS NULL
              OR LENGTH(TRIM(t)) = 0
          ),
          ['article_topics_llm_has_empty_elements'],
          []
        )
      ) AS failure_reasons
    ) AS metadata
  FROM
    needs_processing
  LEFT JOIN
    articles_llm
    USING (id)
  LEFT JOIN
    articles_embedding
    USING (id)
)
SELECT
  *
FROM
  unchanged
UNION ALL
SELECT
  *
FROM
  newly_processed
