{% set llm_model = 'gemini-2.5-pro' %}
{% set embedding_model = 'gemini-embedding-001' %}
{% set sentiment_bound = 1 %}
{%- set ticket_prompt =
  'You are analyzing a Mozilla support ticket. ' ~
  'Extract the following fields and return them as structured data. ' ~
  'ticket_summary_llm: maximum 8 words, clear, factual, in English. ' ~
  'ticket_category_llm: exactly 1 reusable classification label, preferably 1 word, maximum 2 words. ' ~
  'ticket_language_llm: BCP 47 language tag of the ticket text, including region, e.g. en-US. ' ~
  'ticket_entities_llm: array of up to 3 important normalized entities (e.g. product names, features, error codes), short, deduplicated. ' ~
  'ticket_topics_llm: array of up to 3 normalized topics, preferably 1 word, maximum 2 words each, reusable across similar texts, suitable as classification labels. ' ~
  'ticket_sentiment_score: float from -' ~ sentiment_bound ~ ' to ' ~ sentiment_bound ~ ' where -' ~ sentiment_bound ~ ' is very negative, 0 is neutral, ' ~ sentiment_bound ~ ' is very positive. '
-%}
WITH target_tickets AS (
  SELECT
    id AS ticket_id
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket`
  WHERE
    DATE(created_at) = @submission_date
),
appbot_class AS (
  SELECT
    ticket_id,
    mozfun.customer_experience.classify_appbot_group(ARRAY_AGG(tag)).ticket_group AS ticket_group
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
  WHERE
    ticket_id IN (SELECT ticket_id FROM target_tickets)
  GROUP BY
    ticket_id
),
/*
Count reopens per ticket based on query for reopens https://fivetran.com/docs/connectors/applications/zendesk/ticket-metrics-queries
*/
reopens AS (
  SELECT
    ticket_id,
    COUNT(ticket_id) AS reopen_count
  FROM
    (
      SELECT
        ticket_id,
        value AS status,
        LAG(value) OVER (PARTITION BY ticket_id ORDER BY updated) AS prev_status
      FROM
        `moz-fx-data-shared-prod.zendesk_syndicate.ticket_field_history`
      WHERE
        field_name = 'status'
        AND ticket_id IN (SELECT ticket_id FROM target_tickets)
    ) statuses
  WHERE
    prev_status = 'solved'
    AND status = 'open'
  GROUP BY
    ticket_id
),
/* flag automation tags*/
human_auto AS (
  SELECT
    ticket_id,
    IF(mozfun.customer_experience.is_automated(ARRAY_AGG(tag)), 1, 0) AS automation_class
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
  WHERE
    ticket_id IN (SELECT ticket_id FROM target_tickets)
  GROUP BY
    ticket_id
),
/* adjust flags for reopens */
human_auto_flag AS (
  SELECT
    human_auto.ticket_id,
    automation_class,
    reopen_count,
    CASE -- recategorize automated tickets that were reopened
      WHEN automation_class = 1
        AND COALESCE(reopen_count, 0) > 0
        THEN 0
      ELSE automation_class
    END AS automation_category
  FROM
    human_auto
  LEFT JOIN
    reopens
    ON human_auto.ticket_id = reopens.ticket_id
),
solve_dates AS (
  SELECT
    ticket_id,
    MIN(CASE WHEN value = 'solved' THEN updated END) AS first_solved_at,
    MAX(CASE WHEN value = 'solved' THEN updated END) AS last_solved_at,
    MAX(CASE WHEN value = 'closed' THEN updated END) AS closed_at
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket_field_history`
  WHERE
    ticket_id IN (SELECT ticket_id FROM target_tickets)
  GROUP BY
    ticket_id
),
/* Main ticket-level dataset we will be using */
zendesk AS (
  SELECT
    ticket.created_at,
    status,
    sd.first_solved_at,
    sd.last_solved_at,
    sd.closed_at,
    ticket.id AS ticket_id,
    ticket.subject AS title,
    custom_appbot_star_rating AS star_rating,
    ticket.description AS content,
    ticket.custom_product AS product,
    u.locale,
    custom_category,
    g.name AS group_name,
    custom_country,
    via_channel,
    ha.automation_category
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket` ticket
  LEFT JOIN
    appbot_class
    ON ticket.id = appbot_class.ticket_id
  LEFT JOIN
    `moz-fx-data-shared-prod.zendesk_syndicate.group` g
    ON ticket.group_id = g.id
  LEFT JOIN
    `moz-fx-data-shared-prod.zendesk_syndicate.user` u
    ON ticket.submitter_id = u.id
  LEFT JOIN
    solve_dates sd
    ON ticket.id = sd.ticket_id
  LEFT JOIN
    human_auto_flag ha
    ON ticket.id = ha.ticket_id
  WHERE
    DATE(ticket.created_at) = @submission_date
    AND COALESCE(ticket_group, 'All Other Tickets') != 'Appbot - Non-English'
    AND g.name NOT IN ('Sumo Test', 'VPN QA')
    AND status != 'deleted'
    AND ticket.id NOT IN (
      SELECT
        ticket_id
      FROM
        `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
      WHERE
        (REGEXP_CONTAINS(tag, r'(^|[-_])test([-_]|$)') OR tag = 'qatest') -- filter out test tickets
        AND ticket_id IN (SELECT ticket_id FROM target_tickets)
    )
),
zendesk_llm AS (
  SELECT
    ticket_id,
    AI.GENERATE(
      prompt => CONCAT('{{ ticket_prompt }}', 'Title: ', title, '\n', 'Content: ', content),
      endpoint => '{{ llm_model }}',
      output_schema => 'ticket_summary_llm STRING, ticket_category_llm STRING, ticket_language_llm STRING, ticket_entities_llm ARRAY<STRING>, ticket_topics_llm ARRAY<STRING>, ticket_sentiment_score FLOAT64'
    ) AS llm_result
  FROM
    zendesk
),
zendesk_embedding AS (
  SELECT
    ticket_id,
    AI.EMBED(CONCAT(title, ' ', content), endpoint => '{{ embedding_model }}').result AS embedding
  FROM
    zendesk
)
SELECT
  DATE(zendesk.created_at) AS creation_date,
  zendesk.ticket_id,
  zendesk.title,
  zendesk.content,
  zendesk.status,
  last_solved_at,
  closed_at,
  TIMESTAMP_DIFF(first_solved_at, zendesk.created_at, SECOND) AS resolution_latency_seconds,
  star_rating,
  product,
  locale,
  custom_country,
  group_name,
  via_channel,
  custom_category,
  automation_category,
  'ticket' AS type,
  zendesk_llm.llm_result.ticket_summary_llm,
  zendesk_llm.llm_result.ticket_category_llm,
  zendesk_llm.llm_result.ticket_language_llm,
  zendesk_llm.llm_result.ticket_entities_llm,
  zendesk_llm.llm_result.ticket_topics_llm,
  IF(
    zendesk_llm.llm_result.ticket_sentiment_score
    BETWEEN - {{ sentiment_bound }}
    AND {{ sentiment_bound }},
    zendesk_llm.llm_result.ticket_sentiment_score,
    NULL
  ) AS ticket_sentiment_score,
  embedding,
  STRUCT(
    ['title', 'content'] AS input_fields,
    LENGTH(CONCAT(zendesk.title, ' ', zendesk.content)) AS input_char_count,
    '{{ llm_model }}' AS model_version,
    '{{ embedding_model }}' AS embedding_version,
    'v1' AS prompt_version,
    CURRENT_TIMESTAMP() AS analysis_timestamp,
    embedding IS NOT NULL AS embedding_succeeded,
    ARRAY_CONCAT(
      IF(embedding IS NULL, ['embedding_missing'], []),
      IF(
        zendesk_llm.llm_result.ticket_summary_llm IS NULL
        OR LENGTH(TRIM(zendesk_llm.llm_result.ticket_summary_llm)) = 0,
        ['ticket_summary_llm_missing'],
        []
      ),
      IF(
        zendesk_llm.llm_result.ticket_category_llm IS NULL
        OR LENGTH(TRIM(zendesk_llm.llm_result.ticket_category_llm)) = 0,
        ['ticket_category_llm_missing'],
        []
      ),
      IF(
        zendesk_llm.llm_result.ticket_language_llm IS NULL
        OR LENGTH(TRIM(zendesk_llm.llm_result.ticket_language_llm)) = 0,
        ['ticket_language_llm_missing'],
        []
      ),
      IF(
        zendesk_llm.llm_result.ticket_sentiment_score IS NULL,
        ['ticket_sentiment_score_missing'],
        []
      ),
      IF(
        zendesk_llm.llm_result.ticket_sentiment_score NOT BETWEEN - {{ sentiment_bound }}
        AND {{ sentiment_bound }},
        ['ticket_sentiment_score_out_of_range'],
        []
      ),
      IF(
        zendesk_llm.llm_result.ticket_entities_llm IS NULL
        OR ARRAY_LENGTH(zendesk_llm.llm_result.ticket_entities_llm) = 0,
        ['ticket_entities_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(zendesk_llm.llm_result.ticket_entities_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['ticket_entities_llm_has_empty_elements'],
        []
      ),
      IF(
        zendesk_llm.llm_result.ticket_topics_llm IS NULL
        OR ARRAY_LENGTH(zendesk_llm.llm_result.ticket_topics_llm) = 0,
        ['ticket_topics_llm_missing'],
        []
      ),
      IF(
        EXISTS (
          SELECT
            1
          FROM
            UNNEST(zendesk_llm.llm_result.ticket_topics_llm) t
          WHERE
            t IS NULL
            OR LENGTH(TRIM(t)) = 0
        ),
        ['ticket_topics_llm_has_empty_elements'],
        []
      )
    ) AS failure_reasons
  ) AS metadata
FROM
  zendesk
LEFT JOIN
  zendesk_llm
  USING (ticket_id)
LEFT JOIN
  zendesk_embedding
  USING (ticket_id)
