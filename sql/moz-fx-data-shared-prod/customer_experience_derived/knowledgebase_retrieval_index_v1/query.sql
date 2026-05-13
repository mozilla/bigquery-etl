{% set llm_model = 'gemini-2.5-pro' %}
{% set embedding_model = 'gemini-embedding-001' %}
{% set sentiment_bound = 1 %}
{%- set article_prompt =
  'You are analyzing a Mozilla Knowledge Base support article. ' ~
  'Extract the following fields and return them as structured data. ' ~
  'article_summary_llm: maximum 8 words, clear, factual, in English. ' ~
  'article_category_llm: exactly 1 reusable classification label, preferably 1 word, maximum 2 words. ' ~
  'article_language_llm: BCP 47 language tag of the article text, including region, e.g. en-US. ' ~
  'article_entities_llm: array of up to 3 important normalized entities (e.g. product names, features, error codes), short, deduplicated. ' ~
  'article_topics_llm: array of up to 3 normalized topics, preferably 1 word, maximum 2 words each, reusable across similar texts, suitable as classification labels. ' ~
  'article_sentiment_score: float from -' ~ sentiment_bound ~ ' to ' ~ sentiment_bound ~ ' where -' ~ sentiment_bound ~ ' is very negative, 0 is neutral, ' ~ sentiment_bound ~ ' is very positive. '
-%}
WITH articles AS (
  SELECT
    id,
    title,
    slug,
    is_template,
    is_localizable,
    locale,
    html AS content,
    category,
    allow_discussion,
    needs_change,
    needs_change_comment,
    share_link,
    display_order,
    current_revision_id,
    latest_localizable_revision_id,
    parent_id,
    products,
    topics,
    last_updated,
    num_pageviews_last_7_days,
    num_pageviews_last_30_days,
    num_pageviews_last_90_days,
    num_pageviews_last_365_days
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document_plus`
  WHERE
    parent_id IS NULL -- original non-translated articles
    AND is_archived = FALSE
    AND category < 30 -- Only categories relevant to the users. See schema description for further details.
    -- categories https://github.com/mozilla/kitsune/blob/3ddd61a2f32eb486388366874d42f9a860e357d8/kitsune/wiki/config.py#L87
    AND html NOT LIKE '%REDIRECT%'
    AND products NOT LIKE '%thunderbird%'
),
articles_llm AS (
  SELECT
    id,
    AI.GENERATE(
      prompt => CONCAT('{{ article_prompt }}', 'Title: ', title, '\n', 'Content: ', content),
      endpoint => '{{ llm_model }}',
      output_schema => 'article_summary_llm STRING, article_category_llm STRING, article_language_llm STRING, article_entities_llm ARRAY<STRING>, article_topics_llm ARRAY<STRING>, article_sentiment_score FLOAT64'
    ) AS llm_result
  FROM
    articles
),
articles_embedding AS (
  SELECT
    id,
    AI.EMBED(CONCAT(title, ' ', content), endpoint => '{{ embedding_model }}').result AS embedding
  FROM
    articles
)
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
  num_pageviews_last_7_days,
  num_pageviews_last_30_days,
  num_pageviews_last_90_days,
  num_pageviews_last_365_days,
  'article' AS type,
  articles_llm.llm_result.article_summary_llm,
  articles_llm.llm_result.article_category_llm,
  articles_llm.llm_result.article_language_llm,
  articles_llm.llm_result.article_entities_llm,
  articles_llm.llm_result.article_topics_llm,
  IF(
    articles_llm.llm_result.article_sentiment_score
    BETWEEN - {{ sentiment_bound }}
    AND {{ sentiment_bound }},
    articles_llm.llm_result.article_sentiment_score,
    NULL
  ) AS article_sentiment_score,
  embedding,
  STRUCT(
    ['title', 'content'] AS input_fields,
    LENGTH(CONCAT(articles.title, ' ', articles.content)) AS input_char_count,
    '{{ llm_model }}' AS model_version,
    '{{ embedding_model }}' AS embedding_version,
    'v1' AS prompt_version,
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
        articles_llm.llm_result.article_language_llm IS NULL
        OR LENGTH(TRIM(articles_llm.llm_result.article_language_llm)) = 0,
        ['article_language_llm_missing'],
        []
      ),
      IF(
        articles_llm.llm_result.article_sentiment_score IS NULL,
        ['article_sentiment_score_missing'],
        []
      ),
      IF(
        articles_llm.llm_result.article_sentiment_score NOT BETWEEN - {{ sentiment_bound }}
        AND {{ sentiment_bound }},
        ['article_sentiment_score_out_of_range'],
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
  articles
LEFT JOIN
  articles_llm
  USING (id)
LEFT JOIN
  articles_embedding
  USING (id)
