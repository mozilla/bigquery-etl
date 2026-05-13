{% set llm_model = 'gemini-2.5-pro' %}
{% set embedding_model = 'gemini-embedding-001' %}
{% set sentiment_bound = 1 %}
{%- set kb_prompt =
  'You are analyzing a Mozilla Knowledge Base article. ' ~
  'Extract the following fields and return them as structured data. ' ~
  'article_summary_llm: maximum 8 words, clear, factual, in English. ' ~
  'article_category_llm: exactly 1 reusable classification label, preferably 1 word, maximum 2 words. ' ~
  'article_language_llm: BCP 47 language tag of the article text, including region, e.g. en-US. ' ~
  'article_entities_llm: array of up to 3 important normalized entities (e.g. product names, features, error codes), short, deduplicated. ' ~
  'article_topics_llm: array of up to 3 normalized topics, preferably 1 word, maximum 2 words each, reusable across similar texts, suitable as classification labels. ' ~
  'article_sentiment_score: float from -' ~ sentiment_bound ~ ' to ' ~ sentiment_bound ~ ' where -' ~ sentiment_bound ~ ' is very negative, 0 is neutral, ' ~ sentiment_bound ~ ' is very positive. '
-%}
WITH kb_articles AS (
    SELECT
        *
    FROM `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document_plus`
    WHERE parent_id is null -- original non-translated articles
        AND is_archived = False
        AND category < 30
        AND html not like '%REDIRECT%'
        AND LOWER(products) NOT LIKE '%thunderbird%'
),
kb_articles_llm AS (
  SELECT
    id,
    AI.GENERATE(
      prompt => CONCAT('{{ kb_prompt }}', 'Title: ', title, '\n', 'Content: ', html),
      endpoint => '{{ llm_model }}',
      output_schema => 'article_summary_llm STRING, article_category_llm STRING, article_language_llm STRING, article_entities_llm ARRAY<STRING>, article_topics_llm ARRAY<STRING>, article_sentiment_score FLOAT64'
    ) AS llm_result
  FROM
    kb_articles
),
kb_articles_embedding AS (
  SELECT
    id,
    AI.EMBED(CONCAT(title, ' ', content), endpoint => '{{ embedding_model }}').result AS embedding
  FROM
    kb_articles
)
SELECT
  DATE(kb_articles.created_at) AS creation_date,
  kb_articles.article_id,
  kb_articles.title,
  kb_articles.content,
  kb_articles.status,
  last_solved_at,
  closed_at,
  TIMESTAMP_DIFF(first_solved_at, kb_articles.created_at, SECOND) AS resolution_latency_seconds,
  star_rating,
  product,
  locale,
  custom_country,
  group_name,
  via_channel,
  custom_category,
  automation_category,
  'article' AS type,
  kb_articles_llm.llm_result.article_summary_llm,
  kb_articles_llm.llm_result.article_category_llm,
  kb_articles_llm.llm_result.article_language_llm,
  kb_articles_llm.llm_result.article_entities_llm,
  kb_articles_llm.llm_result.article_topics_llm,
  IF(
    kb_articles_llm.llm_result.article_sentiment_score
    BETWEEN - {{ sentiment_bound }}
    AND {{ sentiment_bound }},
    kb_articles_llm.llm_result.article_sentiment_score,
    NULL
  ) AS article_sentiment_score,
  embedding,
  STRUCT(
    ['title', 'content'] AS input_fields,
    LENGTH(CONCAT(kb_articles.title, ' ', kb_articles.content)) AS input_char_count,
    '{{ llm_model }}' AS model_version,
    '{{ embedding_model }}' AS embedding_version,
    'v1' AS prompt_version,
    CURRENT_TIMESTAMP() AS analysis_timestamp,
    embedding IS NOT NULL AS embedding_succeeded,
    ARRAY_CONCAT(
      IF(embedding IS NULL, ['embedding_missing'], []),
      IF(
        kb_articles_llm.llm_result.article_summary_llm IS NULL
        OR LENGTH(TRIM(kb_articles_llm.llm_result.article_summary_llm)) = 0,
        ['article_summary_llm_missing'],
        []
      ),
      IF(
        kb_articles_llm.llm_result.article_category_llm IS NULL
        OR LENGTH(TRIM(kb_articles_llm.llm_result.article_category_llm)) = 0,
        ['article_category_llm_missing'],
        []
      ),
      IF(
        kb_articles_llm.llm_result.article_language_llm IS NULL
        OR LENGTH(TRIM(kb_articles_llm.llm_result.article_language_llm)) = 0,
        ['article_language_llm_missing'],
        []
      ),
      IF(
        kb_articles_llm.llm_result.article_sentiment_score IS NULL,
        ['article_sentiment_score_missing'],
        []
      ),
      IF(
        kb_articles_llm.llm_result.article_sentiment_score NOT BETWEEN - {{ sentiment_bound }}
        AND {{ sentiment_bound }},
        ['article_sentiment_score_out_of_range'],
        []
      ),
      IF(
        kb_articles_llm.llm_result.article_entities_llm IS NULL
        OR ARRAY_LENGTH(kb_articles_llm.llm_result.article_entities_llm) = 0,
        ['article_entities_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(kb_articles_llm.llm_result.article_entities_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['article_entities_llm_has_empty_elements'],
        []
      ),
      IF(
        kb_articles_llm.llm_result.article_topics_llm IS NULL
        OR ARRAY_LENGTH(kb_articles_llm.llm_result.article_topics_llm) = 0,
        ['article_topics_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(kb_articles_llm.llm_result.article_topics_llm) t
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
  kb_articles
LEFT JOIN
  kb_articles_llm
  USING (article_id)
LEFT JOIN
  kb_articles_embedding
  USING (article_id)
