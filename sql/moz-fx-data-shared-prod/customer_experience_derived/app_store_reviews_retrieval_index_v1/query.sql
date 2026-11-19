{% set llm_model = 'gemini-3.8-flash' %}
{% set embedding_model = 'gemini-embedding-001' %}
{% set sentiment_bound = 1 %}
{% set review_categories = ['complaint', 'bug', 'request', 'praise', 'question'] %}
{% set prompt_version = 'v1' %}
{#- Block assignment: one instruction per line. Rendered into a triple-quoted SQL string,
    which allows embedded newlines. Instructions go after the review text, following
    https://aistudio.google.com/docs/prompting-strategies: "supply all the context first.
    Place your specific instructions or questions at the very end of the prompt."
    format:off avoids reformatting this text as SQL.
    Avoid using backslashes and triple quotes in this prompt so it renders correctly. -#}
-- format:off
{% set review_prompt -%}
You are analyzing a single user review of a Mozilla app, published on the Apple App Store.
The input contains the review title and body only.
Reviews are short, informal, may be written in any language, and often mix praise, complaints, bug reports and feature requests.
Base every field on the review text only. Do not infer anything from a star rating, the app listing, or developer replies, which are not provided.
Interpret the review using context and cues to distinguish literal meaning from sarcasm, irony, or exaggeration.
Always prefer the interpretation that is best supported by the evidence.
Extract the following fields and return them as structured data:
review_summary_llm: maximum 8 words, clear, factual, in English, describing the main point of the review.
review_category_llm: exactly one of [{{ review_categories | join(', ') }}], written exactly as shown, lowercase. Return only one of these values and no other value. Use "bug" for something that does not work, including problems that started after an update.
review_language_llm: BCP 47 tag with language and region, e.g. en-US, zh-CN. When the region cannot be determined, use the most common region for that language. For an unknown language use und.
review_entities_llm: array of up to 3 important normalized entities (e.g. product names, features, error codes, companies, websites) that the review is about, short, deduplicated.
review_topics_llm: array of up to 3 broad product areas, preferably 1 word, maximum 2 words each, reusable across similar texts, suitable as classification labels (e.g. ui, updates, privacy, tabs, performance).
review_sentiment_score: float from -{{ sentiment_bound }} to {{ sentiment_bound }} where -{{ sentiment_bound }} is very negative, 0 is neutral, {{ sentiment_bound }} is very positive.
{%- endset %}
-- format:on
WITH app_store_reviews AS (
  SELECT
    id,
    TIMESTAMP_TRUNC(created_date, SECOND) AS creation_date,
    title,
    body,
    IFNULL(title, '') AS title_text,
    IFNULL(body, '') AS body_text
  FROM
    `moz-fx-data-shared-prod.app_store_v2_syndicate.app_store_review`
  WHERE
    DATE(created_date) = @submission_date
),
reviews_llm AS (
  SELECT
    id,
    AI.GENERATE(
      prompt => CONCAT(
        'Title: ',
        title_text,
        '\n',
        'Content: ',
        body_text,
        '\n\n',
        '''{{ review_prompt }}'''
      ),
      endpoint => '{{ llm_model }}',
      output_schema => 'review_summary_llm STRING, review_category_llm STRING, review_language_llm STRING, review_entities_llm ARRAY<STRING>, review_topics_llm ARRAY<STRING>, review_sentiment_score FLOAT64'
    ) AS llm_result
  FROM
    app_store_reviews
),
reviews_embedding AS (
  SELECT
    id,
    AI.EMBED(
      CONCAT(title_text, ' ', body_text),
      endpoint => '{{ embedding_model }}'
    ).result AS embedding
  FROM
    app_store_reviews
)
SELECT
  app_store_reviews.creation_date,
  app_store_reviews.id,
  app_store_reviews.title,
  app_store_reviews.body,
  TRIM(reviews_llm.llm_result.review_summary_llm) AS review_summary_llm,
  LOWER(TRIM(reviews_llm.llm_result.review_category_llm)) AS review_category_llm,
  TRIM(reviews_llm.llm_result.review_language_llm) AS review_language_llm,
  ARRAY(
    SELECT
      LOWER(TRIM(e))
    FROM
      UNNEST(reviews_llm.llm_result.review_entities_llm) AS e
      WITH OFFSET AS pos
    WHERE
      e IS NOT NULL
      AND pos < 3
    ORDER BY
      pos
  ) AS review_entities_llm,
  ARRAY(
    SELECT
      LOWER(TRIM(t))
    FROM
      UNNEST(reviews_llm.llm_result.review_topics_llm) AS t
      WITH OFFSET AS pos
    WHERE
      t IS NOT NULL
      AND pos < 3
    ORDER BY
      pos
  ) AS review_topics_llm,
  IF(
    reviews_llm.llm_result.review_sentiment_score
    BETWEEN - {{ sentiment_bound }}
    AND {{ sentiment_bound }},
    reviews_llm.llm_result.review_sentiment_score,
    NULL
  ) AS review_sentiment_score,
  embedding,
  STRUCT(
    ['title', 'body'] AS input_fields,
    LENGTH(
      CONCAT(app_store_reviews.title_text, ' ', app_store_reviews.body_text)
    ) AS input_char_count,
    '{{ llm_model }}' AS model_version,
    '{{ embedding_model }}' AS embedding_version,
    '{{ prompt_version }}' AS prompt_version,
    CURRENT_TIMESTAMP() AS analysis_timestamp,
    embedding IS NOT NULL AS embedding_succeeded,
    ARRAY_CONCAT(
      IF(
        LENGTH(reviews_llm.llm_result.status) > 0
        OR reviews_llm.llm_result.review_summary_llm IS NULL
        OR reviews_llm.llm_result.review_category_llm IS NULL
        OR reviews_llm.llm_result.review_language_llm IS NULL
        OR reviews_llm.llm_result.review_sentiment_score IS NULL,
        [
          CONCAT(
            'Error ',
            IFNULL(reviews_llm.llm_result.status, ''),
            ':',
            TO_JSON_STRING(reviews_llm.llm_result.full_response)
          )
        ],
        []
      ),
      IF(embedding IS NULL, ['embedding_missing'], []),
      IF(
        reviews_llm.llm_result.review_summary_llm IS NULL
        OR LENGTH(TRIM(reviews_llm.llm_result.review_summary_llm)) = 0,
        ['review_summary_llm_missing'],
        []
      ),
      IF(
        reviews_llm.llm_result.review_category_llm IS NULL
        OR LENGTH(TRIM(reviews_llm.llm_result.review_category_llm)) = 0,
        ['review_category_llm_missing'],
        []
      ),
      IF(
        LOWER(TRIM(reviews_llm.llm_result.review_category_llm)) NOT IN UNNEST(
          [
            {% for c in review_categories %}
              '{{ c }}'
              {% if not loop.last %},
              {% endif %}
            {% endfor %}
          ]
        ),
        ['review_category_llm_not_allowed'],
        []
      ),
      IF(
        reviews_llm.llm_result.review_language_llm IS NULL
        OR LENGTH(TRIM(reviews_llm.llm_result.review_language_llm)) = 0,
        ['review_language_llm_missing'],
        []
      ),
      IF(
        reviews_llm.llm_result.review_sentiment_score IS NULL,
        ['review_sentiment_score_missing'],
        []
      ),
      IF(
        reviews_llm.llm_result.review_sentiment_score NOT BETWEEN - {{ sentiment_bound }}
        AND {{ sentiment_bound }},
        ['review_sentiment_score_out_of_range'],
        []
      ),
      IF(
        reviews_llm.llm_result.review_entities_llm IS NULL
        OR ARRAY_LENGTH(reviews_llm.llm_result.review_entities_llm) = 0,
        ['review_entities_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(reviews_llm.llm_result.review_entities_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['review_entities_llm_has_empty_elements'],
        []
      ),
      IF(
        reviews_llm.llm_result.review_topics_llm IS NULL
        OR ARRAY_LENGTH(reviews_llm.llm_result.review_topics_llm) = 0,
        ['review_topics_llm_missing'],
        []
      ),
      IF(
        ARRAY_LENGTH(reviews_llm.llm_result.review_topics_llm) > 3,
        ['review_topics_llm_over_limit'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(reviews_llm.llm_result.review_topics_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['review_topics_llm_has_empty_elements'],
        []
      )
    ) AS failure_reasons
  ) AS metadata
FROM
  app_store_reviews
LEFT JOIN
  reviews_llm
  USING (id)
LEFT JOIN
  reviews_embedding
  USING (id)
