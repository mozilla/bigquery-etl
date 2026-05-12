WITH kitsune_questions AS (
  SELECT
    q.question_id,
    q.created_date AS question_created_at,
    q.creator_username AS question_creator,
    COALESCE(m.product_mapping, q.product) AS product,
    q.locale,
    q.topic,
    q.tier1_topic,
    q.tier2_topic,
    q.tier3_topic,
    q.title,
    q.question_content AS content
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_questions_plus` q
  LEFT JOIN
    `moz-fx-data-shared-prod.static.cx_product_mappings_v1` m
    ON m.product = q.product
    AND m.source = 'Kitsune'
  WHERE
    q.is_spam = FALSE
    AND q.is_locked = FALSE
    AND DATE(q.created_date) = @submission_date
),
kitsune_answers AS (
  SELECT
    answer_id,
    creator_username AS answer_creator,
    question_id,
    answer_content,
    created_date AS answer_creation_datetime,
    num_helpful_votes,
    num_unhelpful_votes
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_answers_raw`
  WHERE
    is_spam = FALSE
    AND DATE(created_date) >= @submission_date
    AND question_id IN (SELECT question_id FROM kitsune_questions)
),
kitsune_joined AS (
  SELECT
    kitsune_questions.*,
    kitsune_answers.* EXCEPT (question_id)
  FROM
    kitsune_questions
  LEFT JOIN
    kitsune_answers
    USING (question_id)
),
kitsune_questions_distinct AS (
  SELECT DISTINCT
    question_id,
    title,
    content
  FROM
    kitsune_joined
),
kitsune_llm AS (
  SELECT
    question_id,
    AI.GENERATE(
      prompt => CONCAT(
        'You are analyzing a Mozilla Firefox support forum question. ',
        'Extract the following fields and return them as structured data. ',
        'question_summary_llm: maximum 8 words, clear, factual, in English. ',
        'question_category_llm: exactly 1 reusable classification label, preferably 1 word, maximum 2 words. ',
        'question_language_llm: BCP 47 language tag of the question text, including region, e.g. en-US. ',
        'question_entities_llm: array of up to 3 important normalized entities (e.g. product names, features, error codes), short, deduplicated. ',
        'question_topics_llm: array of up to 3 normalized topics, preferably 1 word, maximum 2 words each, ',
        'reusable across similar texts, suitable as classification labels. ',
        'question_sentiment_score: float from -1 to 1 where -1 is very negative, 0 is neutral, 1 is very positive. ',
        'Title: ',
        title,
        '\n',
        'Content: ',
        content
      ),
      endpoint => 'gemini-2.5-pro',
      output_schema => 'question_summary_llm STRING, question_category_llm STRING, question_language_llm STRING, question_entities_llm ARRAY<STRING>, question_topics_llm ARRAY<STRING>, question_sentiment_score FLOAT64'
    ) AS llm_result
  FROM
    kitsune_questions_distinct
),
kitsune_embedding AS (
  SELECT
    question_id,
    AI.EMBED(CONCAT(title, ' ', content), endpoint => 'gemini-embedding-001').result AS embedding
  FROM
    kitsune_questions_distinct
)
SELECT
  DATE(kitsune_joined.question_created_at) AS creation_date,
  question_id,
  answer_id,
  kitsune_joined.title,
  kitsune_joined.content,
  product,
  locale,
  topic,
  tier1_topic,
  tier2_topic,
  tier3_topic,
  answer_content,
  TIMESTAMP_DIFF(answer_creation_datetime, question_created_at, SECOND) AS answer_latency_seconds,
  'question' AS type,
  CASE
    WHEN answer_id IS NULL
      THEN NULL
    ELSE kitsune_joined.question_creator = answer_creator
  END AS is_self_answer,
  (
    STARTS_WITH(product, 'Firefox')
    OR product IN ('Fenix', 'Firefox iOS', 'Klar iOS', 'Klar Android', 'Focus iOS', 'Focus Android')
  ) AS is_firefox_product,
  num_helpful_votes,
  num_unhelpful_votes,
  kitsune_llm.llm_result.question_summary_llm,
  kitsune_llm.llm_result.question_category_llm,
  kitsune_llm.llm_result.question_language_llm,
  kitsune_llm.llm_result.question_entities_llm,
  kitsune_llm.llm_result.question_topics_llm,
  IF(
    kitsune_llm.llm_result.question_sentiment_score
    BETWEEN -1
    AND 1,
    kitsune_llm.llm_result.question_sentiment_score,
    NULL
  ) AS question_sentiment_score,
  embedding,
  STRUCT(
    ['title', 'content'] AS input_fields,
    LENGTH(CONCAT(kitsune_joined.title, ' ', kitsune_joined.content)) AS input_char_count,
    'gemini-2.5-pro' AS model_version,
    'gemini-embedding-001' AS embedding_version,
    'v1' AS prompt_version,
    TIMESTAMP(@submission_date) AS analysis_timestamp,
    embedding IS NOT NULL AS embedding_succeeded,
    ARRAY_CONCAT(
      IF(embedding IS NULL, ['embedding_missing'], []),
      IF(
        kitsune_llm.llm_result.question_summary_llm IS NULL
        OR LENGTH(TRIM(kitsune_llm.llm_result.question_summary_llm)) = 0,
        ['question_summary_llm_missing'],
        []
      ),
      IF(
        kitsune_llm.llm_result.question_category_llm IS NULL
        OR LENGTH(TRIM(kitsune_llm.llm_result.question_category_llm)) = 0,
        ['question_category_llm_missing'],
        []
      ),
      IF(
        kitsune_llm.llm_result.question_language_llm IS NULL
        OR LENGTH(TRIM(kitsune_llm.llm_result.question_language_llm)) = 0,
        ['question_language_llm_missing'],
        []
      ),
      IF(
        kitsune_llm.llm_result.question_sentiment_score IS NULL,
        ['question_sentiment_score_missing'],
        []
      ),
      IF(
        kitsune_llm.llm_result.question_sentiment_score NOT BETWEEN -1
        AND 1,
        ['question_sentiment_score_out_of_range'],
        []
      ),
      IF(
        kitsune_llm.llm_result.question_entities_llm IS NULL
        OR ARRAY_LENGTH(kitsune_llm.llm_result.question_entities_llm) = 0,
        ['question_entities_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(kitsune_llm.llm_result.question_entities_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['question_entities_llm_has_empty_elements'],
        []
      ),
      IF(
        kitsune_llm.llm_result.question_topics_llm IS NULL
        OR ARRAY_LENGTH(kitsune_llm.llm_result.question_topics_llm) = 0,
        ['question_topics_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(kitsune_llm.llm_result.question_topics_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['question_topics_llm_has_empty_elements'],
        []
      )
    ) AS failure_reasons
  ) AS metadata
FROM
  kitsune_joined
LEFT JOIN
  kitsune_llm
  USING (question_id)
LEFT JOIN
  kitsune_embedding
  USING (question_id)
