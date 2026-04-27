WITH questions as (
  SELECT
    question_id,
    DATE(created_date) as creation_date,
    creator_username as content_creator,
    product,
    locale,
    topic,
    tier1_topic,
    tier2_topic,
    tier3_topic,
    title,
    question_content AS content
  FROM `moz-fx-data-shared-prod.sumo_syndicate.kitsune_questions_plus`
  WHERE is_spam = FALSE
    AND is_locked = FALSE
    AND DATE(created_date) >= '2026-03-24' -- TODO: start date. This is temporarily the date of release for Firefox 149 major used for the POC.
)
,answers as (
  SELECT
    answer_id,
    creator_username as answer_creator,
    question_id,
    answer_content,
    created_date as answer_creation_datetime,
    num_helpful_votes,
    num_unhelpful_votes
  FROM `moz-fx-sumo-prod.sumo.kitsune_answers_raw`
  WHERE is_spam = FALSE
    AND DATE(created_date) >= '2026-03-24' -- TODO: start date. This one is the release for Firefox 149 major.
)
,kitsune AS
(
  SELECT
    questions.*,
    answers.* EXCEPT(question_id)
  FROM questions LEFT JOIN answers USING (question_id)
)
,kitsune_llm AS
(
  SELECT
    DISTINCT question_id,
    title,
    content,
    summary_llm,
    category_llm,
    locale_llm,
    entities_llm,
    topics_llm,
    sentiment_score,
    metadata
 FROM
   AI.GENERATE_TABLE(
     MODEL `moz-fx-data-shared-prod.customer_experience_derived.foundational_model`,
     (
       SELECT
         DISTINCT creation_date,
         title,
         content,
         question_id,
         CONCAT(
           'Analyze the following text and return JSON. ',
           'summary_llm: maximum 8 words, clear, factual, normalized. ',
           'category_llm: exactly 1 reusable classification label, with preferably 1 word, maximum 2 words. ',
           'language_llm:  BCP 47 language tag including region, e.g. en-US',
           'entities_llm: array of up to 3 important normalized entities, short, deduplicated.',
           'topics_llm: array of up to 3 normalized topics, as short as possible, preferably 1 word, maximum 2 words each, reusable across similar texts, suitable as classification labels. ',
           'sentiment_score: float from -1 to 1 where -1 is very negative, 0 neutral, 1 very positive. ',
           'metadata: field of type struct containing the following repeatable technical fields: ',
           'input_fields=["title","question_content"], ',
           'input_char_count: integer count of characters of the input text, ',
           'input_quality_score: float from 0 to 1 measuring structure, clarity and usefulness of the source text for analysis, ',
           'model_output_quality_score: float from 0 to 1 evaluating the accuracy and structure and usefulness of your own output, ',
           'model_output_confidence_score: float from 0 to 1 for overall reliability of this analysis, ',
           'foundational_llm_version: "gemini-3.1-pro", ',
           'embedding_llm_version: "gemini-embedding-001", ',
           'prompt_version: "v1", ',
           'ingestion_timestamp: field containing the current timestamp, " ',
           'Text: ', title, ' ', content
         ) AS prompt
       FROM kitsune
     ),
       STRUCT('summary_llm STRING, category_llm STRING, language_llm STRING, entities_llm ARRAY<STRING>, topics_llm ARRAY<STRING>, sentiment_score FLOAT64, metadata STRUCT< input_fields ARRAY<STRING>,  input_char_count INT64, input_quality_score FLOAT64, model_output_quality_score FLOAT64, model_output_confidence_score FLOAT64, foundational_llm_version STRING, embedding_llm_version STRING, prompt_version STRING, ingestion_timestamp STRING>' AS output_schema
     )
   )
)
,kitsune_embedding AS
(
  SELECT
    question_id,
    title,
    content,
    AI.EMBED(
      CONCAT(title, ' ', content),
      endpoint => 'gemini-embedding-001').result
      AS embedding
  FROM kitsune
)
SELECT
  kitsune.creation_date,
  kitsune.title,
  kitsune.content,
  CASE product
      WHEN "firefox" THEN "Firefox Desktop"
      WHEN "mobile" THEN "Fenix" --  TODO: is this how it's setup in Kitsune? What when more product are added? Use lookup?
      WHEN "ios" THEN "Firefox iOS"
      WHEN "firefox-enterprise" THEN "Firefox Enterprise"
      ELSE "Non Firefox"
      END AS product,
  locale,
  topic,
  tier1_topic,
  tier2_topic,
  tier3_topic,
  answer_content,
  'question' AS type,
  IF(forum_post_creator = answer_creator, TRUE, FALSE) as is_self_answer,
  IF(product IN ('firefox-enterprise', 'ios', 'mobile', 'firefox'), TRUE, FALSE) AS is_firefox_product,
  num_helpful_votes,
  num_unhelpful_votes,
  summary_llm,
  category_llm,
  language_llm,
  entities_llm,
  topics_llm,
  sentiment_score,
  EXP(-DATE_DIFF(CURRENT_DATE(), DATE(kitsune.creation_date), DAY) / 7) AS recency_score, -- TODO, define this number of days.
  embedding,
  STRUCT(
    metadata.input_fields,
    metadata.input_char_count,
    metadata.input_quality_score,
    metadata.model_output_quality_score,
    metadata.model_output_confidence_score,
    metadata.model_version,
    metadata.embedding_version,
    metadata.prompt_version,
    metadata.analysis_timestamp,
    CASE
        WHEN category_llm IS NOT NULL AND LENGTH(TRIM(category_llm)) > 0
        AND language_llm IS NOT NULL AND LENGTH(TRIM(language_llm)) > 0
        AND sentiment_score IS NOT NULL AND sentiment_score BETWEEN -1 AND 1
        AND entities_llm IS NOT NULL
        AND ARRAY_LENGTH(entities_llm) > 0
        AND ARRAY_LENGTH(ARRAY(
            SELECT 1 FROM UNNEST(entities_llm) t
            WHERE t IS NOT NULL AND LENGTH(TRIM(t)) > 0
            )) = ARRAY_LENGTH(entities_llm)
        AND entities_llm IS NOT NULL
        AND ARRAY_LENGTH(topics_llm) > 0
        AND ARRAY_LENGTH(ARRAY(
            SELECT 1
            FROM UNNEST(topics_llm) t
            WHERE t IS NOT NULL AND LENGTH(TRIM(t)) > 0
          )) = ARRAY_LENGTH(topics_llm)
        AND sentiment_score IS NOT NULL
      THEN 'SUCCESS'
      ELSE 'FAILED'
    END AS status
  ) AS metadata
    FROM
      kitsune
    LEFT JOIN
      kitsune_llm
    USING (question_id)
    LEFT JOIN
      kitsune_embedding
    USING (question_id)
