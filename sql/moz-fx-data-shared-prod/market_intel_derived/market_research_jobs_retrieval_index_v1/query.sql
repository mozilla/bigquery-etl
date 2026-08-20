{% set llm_model = 'gemini-2.5-pro' %}
{% set embedding_model = 'gemini-embedding-001' %}
{%- set job_prompt =
  'You are analyzing a job posting from a browser or technology company. ' ~
  'Extract the following fields and return them as structured data. ' ~
  'job_summary_llm: maximum 25 words, clear, factual, in English, describing the role and what the hire would work on. ' ~
  'job_category_llm: exactly 1 reusable classification label, preferably 1 word, maximum 2 words, describing the job function (e.g. engineering, design, marketing, legal). ' ~
  'job_topics_llm: array of up to 5 normalized topics, preferably 1 word, maximum 2 words each, reusable across similar texts, suitable as classification labels. ' ~
  'job_entities_llm: array of up to 5 important normalized entities (e.g. technologies, product areas, programming languages, platforms), short, deduplicated. '
-%}
-- Rows scraped in the run being processed. The upstream loader incrementally
-- appends newly-scraped GCS blobs into a cumulative table, keyed by a path
-- that includes scraped_date, so this partition filter selects exactly this
-- week's snapshot of postings.
-- Unlike the releases and blogs indexes, scraped_date is part of this table's
-- grain -- a posting that stays open reappears in every snapshot and is
-- deliberately indexed once per snapshot -- so no anti-join against history
-- is needed: every row selected here is inherently new at this grain.
WITH raw_new AS (
  SELECT
    company,
    source,
    scraped_date,
    job_id,
    title,
    department,
    location,
    offices,
    url,
    first_published,
    updated_at,
    description_html,
    description_text,
    blob_name,
  FROM
    `moz-fx-data-shared-prod.external.market_research_jobs`
  WHERE
    scraped_date = @submission_date
),
-- A single snapshot can in principle re-emit the same posting more than once
-- (e.g. cross-posted to multiple boards); keep the richest copy so the
-- one-row-per-(company, job_id, scraped_date) grain holds.
raw_dedup AS (
  SELECT
    *
  FROM
    raw_new
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        company,
        job_id,
        scraped_date
      ORDER BY
        LENGTH(description_text) DESC,
        url
    ) = 1
),
jobs_llm AS (
  SELECT
    raw_dedup.*,
    AI.GENERATE(
      prompt => CONCAT(
        '{{ job_prompt }}',
        'Company: ',
        company,
        '\n',
        'Title: ',
        title,
        '\n',
        'Description: ',
        description_text
      ),
      endpoint => '{{ llm_model }}',
      output_schema => 'job_summary_llm STRING, job_category_llm STRING, job_topics_llm ARRAY<STRING>, job_entities_llm ARRAY<STRING>'
    ) AS llm_result,
  FROM
    raw_dedup
),
jobs_embedding AS (
  SELECT
    jobs_llm.*,
    AI.EMBED(
      CONCAT(title, ' ', description_text),
      endpoint => '{{ embedding_model }}'
    ).result AS embedding,
  FROM
    jobs_llm
)
SELECT
  scraped_date,
  company,
  source,
  job_id,
  title,
  department,
  location,
  offices,
  url,
  first_published,
  updated_at,
  description_html,
  description_text,
  blob_name,
  llm_result.job_summary_llm,
  llm_result.job_category_llm,
  llm_result.job_topics_llm,
  llm_result.job_entities_llm,
  embedding,
  STRUCT(
    ['title', 'description_text'] AS input_fields,
    LENGTH(CONCAT(title, ' ', description_text)) AS input_char_count,
    '{{ llm_model }}' AS model_version,
    '{{ embedding_model }}' AS embedding_version,
    'v1' AS prompt_version,
    CURRENT_TIMESTAMP() AS analysis_timestamp,
    embedding IS NOT NULL AS embedding_succeeded,
    ARRAY_CONCAT(
      IF(embedding IS NULL, ['embedding_missing'], []),
      IF(
        llm_result.job_summary_llm IS NULL
        OR LENGTH(TRIM(llm_result.job_summary_llm)) = 0,
        ['job_summary_llm_missing'],
        []
      ),
      IF(
        llm_result.job_category_llm IS NULL
        OR LENGTH(TRIM(llm_result.job_category_llm)) = 0,
        ['job_category_llm_missing'],
        []
      ),
      IF(
        llm_result.job_topics_llm IS NULL
        OR ARRAY_LENGTH(llm_result.job_topics_llm) = 0,
        ['job_topics_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(llm_result.job_topics_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['job_topics_llm_has_empty_elements'],
        []
      ),
      IF(
        llm_result.job_entities_llm IS NULL
        OR ARRAY_LENGTH(llm_result.job_entities_llm) = 0,
        ['job_entities_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(llm_result.job_entities_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['job_entities_llm_has_empty_elements'],
        []
      )
    ) AS failure_reasons
  ) AS metadata
FROM
  jobs_embedding
