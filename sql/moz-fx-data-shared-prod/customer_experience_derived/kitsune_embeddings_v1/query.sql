WITH kitsune AS (
 SELECT
   created_date AS creation_date,
   title,
   question_content AS content,
   locale,
   product,
   topic,
   is_solved,
   num_votes AS votes
 FROM
   `moz-fx-data-shared-prod.sumo_syndicate.kitsune_questions`
 WHERE
   DATE(creation_date) = @submission_date
)
,
kitsune_generated AS
(
 SELECT
   creation_date,
   title,
   content,
   summary_generated,
   category_generated,
   language_generated,
   entities_generated,
   topics_generated,
   sentiment_score,
   metadata
 FROM
   AI.GENERATE_TABLE(
     MODEL `moz-fx-dev-lvargas-dev.customer_experience.gemini_model`,
     (
       SELECT
         creation_date,
         title,
         content,
         CONCAT(
           'Analyze the following text and return JSON. ',
           'summary_generated: maximum 20 words, clear, factual, normalized. ',
           'category_generated: exactly 1 reusable classification label, with preferably 1 word, maximum 2 words. ',
           'language_generated: ISO language code like en, es, fr. ',
           'entities_generated: array of up to 5 important normalized entities, short, deduplicated.',
           'topics_generated: array of up to 3 normalized topics, as short as possible, preferably 1 word, maximum 3 words, reusable across similar texts, suitable as classification labels. ',
           'sentiment_score float from -1 to 1 where -1 is very negative, 0 neutral, 1 very positive. ',
           'metadata: field of type struct containing the following repeatable technical fields: ',
           'input_fields=["title","content"], ',
           'input_char_count: integer count of characters of the input text, ',
           'input_quality_score: float from 0 to 1 measuring structure, clarity and usefulness of the source text for analysis, ',
           'model_output_quality_score: float from 0 to 1 evaluating the accuracy and structure and usefulness of your own output, ',
           'model_output_confidence_score: float from 0 to 1 for overall reliability of this analysis, ',
           'model_version: "gemini-2.5-pro", ',
           'embedding_version: "gemini-embedding-001", ',
           'prompt_version: "v1", ',
           'analysis_timestamp timestamp: field containing the current timestamp, " ',
           'Text: ', title, ' ', content
         ) AS prompt
       FROM kitsune
     ),
       STRUCT('summary_generated STRING, category_generated STRING, language_generated STRING, entities_generated ARRAY<STRING>, topics_generated ARRAY<STRING>, sentiment_score FLOAT64, metadata STRUCT< input_fields ARRAY<STRING>,  input_char_count INT64, input_quality_score FLOAT64, model_output_quality_score FLOAT64, model_output_confidence_score FLOAT64, model_version STRING, embedding_version STRING, prompt_version STRING, analysis_timestamp STRING>' AS output_schema
     )
   )
),
kitsune_embedding AS
(
  SELECT
    creation_date,
    title,
    content,
    AI.EMBED(CONCAT(title, ' ', content), endpoint => 'gemini-embedding-001').result AS embedding
  FROM kitsune
)
SELECT
  kitsune.creation_date,
  kitsune.title,
  kitsune.content,
  locale,
  product,
  topic,
  'question' AS type,
  is_solved,
  votes,
  summary_generated,
  category_generated,
  language_generated,
  entities_generated,
  topics_generated,
  sentiment_score,
  embedding,
  EXP(-DATE_DIFF(CURRENT_DATE(), DATE(kitsune.creation_date), DAY) / 7) AS recency_score,
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
      WHEN category_generated IS NOT NULL AND LENGTH(TRIM(category_generated)) > 0
      AND language_generated IS NOT NULL AND LENGTH(TRIM(language_generated)) > 0
      AND sentiment_score IS NOT NULL AND sentiment_score BETWEEN -1 AND 1
      AND entities_generated IS NOT NULL
      AND ARRAY_LENGTH(entities_generated) > 0
      AND ARRAY_LENGTH(ARRAY(
            SELECT 1
            FROM UNNEST(entities_generated) t
            WHERE t IS NOT NULL AND LENGTH(TRIM(t)) > 0
          )) = ARRAY_LENGTH(entities_generated)
      AND entities_generated IS NOT NULL
      AND ARRAY_LENGTH(topics_generated) > 0
      AND ARRAY_LENGTH(ARRAY(
            SELECT 1
            FROM UNNEST(topics_generated) t
            WHERE t IS NOT NULL AND LENGTH(TRIM(t)) > 0
          )) = ARRAY_LENGTH(topics_generated)
      THEN 'SUCCESS'
      ELSE 'FAILED'
    END AS status
  ) AS metadata
FROM
 kitsune
 LEFT JOIN
 kitsune_generated
 -- Use SHA-256 to generate a stable key to match even if formatting differs.
  ON TO_HEX(SHA256(CONCAT(
     COALESCE(LOWER(TRIM(kitsune.title)), '∅'), '|',
     COALESCE(LOWER(TRIM(kitsune.content)), '∅'), '|',
     COALESCE(CAST(kitsune.creation_date AS STRING), '∅'), '|'
  ))) =
  TO_HEX(SHA256(CONCAT(
     COALESCE(LOWER(TRIM(kitsune_generated.title)), '∅'), '|',
     COALESCE(LOWER(TRIM(kitsune_generated.content)), '∅'), '|',
     COALESCE(CAST(kitsune_generated.creation_date AS STRING), '∅'), '|'
)))
LEFT JOIN kitsune_embedding
  ON TO_HEX(SHA256(CONCAT(
     COALESCE(LOWER(TRIM(kitsune.title)), '∅'), '|',
     COALESCE(LOWER(TRIM(kitsune.content)), '∅'), '|',
     COALESCE(CAST(kitsune.creation_date AS STRING), '∅'), '|'
  ))) =
  TO_HEX(SHA256(CONCAT(
     COALESCE(LOWER(TRIM(kitsune_generated.title)), '∅'), '|',
     COALESCE(LOWER(TRIM(kitsune_generated.content)), '∅'), '|',
     COALESCE(CAST(kitsune_generated.creation_date AS STRING), '∅'), '|'
)))
