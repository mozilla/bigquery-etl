{% set llm_model = 'gemini-2.5-pro' %}
{% set embedding_model = 'gemini-embedding-001' %}
{%- set post_prompt =
  'You are analyzing a browser vendor blog post. ' ~
  'Extract the following fields and return them as structured data. ' ~
  'post_summary_llm: maximum 25 words, clear, factual, in English, describing what the post announces or argues. ' ~
  'post_category_llm: exactly 1 reusable classification label, preferably 1 word, maximum 2 words, describing the dominant theme of the post (e.g. security, performance, privacy, marketing). ' ~
  'post_topics_llm: array of up to 5 normalized topics, preferably 1 word, maximum 2 words each, reusable across similar texts, suitable as classification labels. ' ~
  'post_entities_llm: array of up to 5 important normalized entities (e.g. feature names, web platform APIs, product names, organizations), short, deduplicated. '
-%}
-- Rows scraped in the run being processed. The upstream loader incrementally
-- appends newly-scraped GCS blobs into a cumulative table, so this is not a
-- clean single-day arrivals feed -- a scraped_date batch can include any
-- post that happened to be scraped that week, old or new.
WITH raw_new AS (
  SELECT
    browser,
    release_date,
    scraped_date,
    source_url,
    source_type,
    title,
    raw_text,
    blob_name,
  FROM
    `moz-fx-data-shared-prod.external.market_research_blogs`
  WHERE
    scraped_date = @submission_date
),
-- Identities already enriched by a previous run. Self-referencing anti-join
-- lookup: the `<=` predicate satisfies this table's required partition filter
-- while still scanning all prior history. On the first-ever run the table
-- exists but is empty, so this yields zero rows and everything is treated as
-- new -- which is the desired behaviour.
existing_keys AS (
  SELECT DISTINCT
    source_url,
    TRUE AS already_indexed,
  FROM
    `moz-fx-data-shared-prod.market_intel_derived.market_research_blogs_retrieval_index_v1`
  WHERE
    scraped_date <= @submission_date
),
-- A single scrape can in principle re-emit the same source_url more than
-- once (e.g. the same post surfacing in two feeds); keep the richest copy so
-- the one-row-per-source_url grain holds.
raw_dedup AS (
  SELECT
    *
  FROM
    raw_new
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY source_url ORDER BY LENGTH(raw_text) DESC, title) = 1
),
-- Only enrich posts that have never been indexed. AI.GENERATE and AI.EMBED
-- are the expensive part of this query, so re-embedding a post that has not
-- changed since a prior scrape is pure waste.
new_only AS (
  SELECT
    raw_dedup.*
  FROM
    raw_dedup
  LEFT JOIN
    existing_keys
    -- source_url is a scraped value and can in principle be NULL, so coalesce
    -- to a sentinel: a NULL-keyed row must still match its prior-run self
    -- rather than be re-enriched on every scrape.
    ON COALESCE(raw_dedup.source_url, '') = COALESCE(existing_keys.source_url, '')
  WHERE
    existing_keys.already_indexed IS NULL
),
blogs_llm AS (
  SELECT
    new_only.*,
    AI.GENERATE(
      prompt => CONCAT(
        '{{ post_prompt }}',
        'Browser: ',
        browser,
        '\n',
        'Title: ',
        title,
        '\n',
        'Content: ',
        raw_text
      ),
      endpoint => '{{ llm_model }}',
      output_schema => 'post_summary_llm STRING, post_category_llm STRING, post_topics_llm ARRAY<STRING>, post_entities_llm ARRAY<STRING>'
    ) AS llm_result,
  FROM
    new_only
),
blogs_embedding AS (
  SELECT
    blogs_llm.*,
    AI.EMBED(CONCAT(title, ' ', raw_text), endpoint => '{{ embedding_model }}').result AS embedding,
  FROM
    blogs_llm
)
SELECT
  scraped_date,
  browser,
  release_date,
  source_type,
  source_url,
  title,
  raw_text,
  blob_name,
  llm_result.post_summary_llm,
  llm_result.post_category_llm,
  llm_result.post_topics_llm,
  llm_result.post_entities_llm,
  embedding,
  STRUCT(
    ['title', 'raw_text'] AS input_fields,
    LENGTH(CONCAT(title, ' ', raw_text)) AS input_char_count,
    '{{ llm_model }}' AS model_version,
    '{{ embedding_model }}' AS embedding_version,
    'v1' AS prompt_version,
    CURRENT_TIMESTAMP() AS analysis_timestamp,
    embedding IS NOT NULL AS embedding_succeeded,
    ARRAY_CONCAT(
      IF(embedding IS NULL, ['embedding_missing'], []),
      IF(
        llm_result.post_summary_llm IS NULL
        OR LENGTH(TRIM(llm_result.post_summary_llm)) = 0,
        ['post_summary_llm_missing'],
        []
      ),
      IF(
        llm_result.post_category_llm IS NULL
        OR LENGTH(TRIM(llm_result.post_category_llm)) = 0,
        ['post_category_llm_missing'],
        []
      ),
      IF(
        llm_result.post_topics_llm IS NULL
        OR ARRAY_LENGTH(llm_result.post_topics_llm) = 0,
        ['post_topics_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(llm_result.post_topics_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['post_topics_llm_has_empty_elements'],
        []
      ),
      IF(
        llm_result.post_entities_llm IS NULL
        OR ARRAY_LENGTH(llm_result.post_entities_llm) = 0,
        ['post_entities_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(llm_result.post_entities_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['post_entities_llm_has_empty_elements'],
        []
      )
    ) AS failure_reasons
  ) AS metadata
FROM
  blogs_embedding
