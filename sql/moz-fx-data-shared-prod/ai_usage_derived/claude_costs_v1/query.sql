WITH pricing AS (
  SELECT
    model_prefix,
    input_mtok,
    cache_write_5m_mtok,
    cache_write_1h_mtok,
    cache_read_mtok,
    output_mtok,
    10.00 AS web_search_per_1k
  FROM
    UNNEST(
      [
        STRUCT(
          'claude-opus-4-6' AS model_prefix,
          5.00 AS input_mtok,
          6.25 AS cache_write_5m_mtok,
          10.00 AS cache_write_1h_mtok,
          0.50 AS cache_read_mtok,
          25.00 AS output_mtok
        ),
        STRUCT('claude-opus-4-5', 5.00, 6.25, 10.00, 0.50, 25.00),
        STRUCT('claude-opus-4-1', 15.00, 18.75, 30.00, 1.50, 75.00),
        STRUCT('claude-opus-4-', 15.00, 18.75, 30.00, 1.50, 75.00),
        STRUCT('claude-sonnet-4-6', 3.00, 3.75, 6.00, 0.30, 15.00),
        STRUCT('claude-sonnet-4-5', 3.00, 3.75, 6.00, 0.30, 15.00),
        STRUCT('claude-sonnet-4-', 3.00, 3.75, 6.00, 0.30, 15.00),
        STRUCT('claude-3-7-sonnet', 3.00, 3.75, 6.00, 0.30, 15.00),
        STRUCT('claude-haiku-4-5', 1.00, 1.25, 2.00, 0.10, 5.00),
        STRUCT('claude-3-5-haiku', 0.80, 1.00, 1.60, 0.08, 4.00),
        STRUCT('claude-3-opus', 15.00, 18.75, 30.00, 1.50, 75.00),
        STRUCT('claude-3-haiku', 0.25, 0.30, 0.50, 0.03, 1.25)
      ]
    )
),
  -- Join each usage row to the most specific matching pricing prefix.
  -- QUALIFY keeps the single best match (longest prefix wins).
usage_with_pricing AS (
  SELECT
    u.date,
    u.api_key_id,
    u.model,
    u.uncached_input_tokens,
    u.cache_creation_5m_input_tokens,
    u.cache_creation_1h_input_tokens,
    u.cache_read_input_tokens,
    u.output_tokens,
    u.web_search_requests,
    p.input_mtok,
    p.cache_write_5m_mtok,
    p.cache_write_1h_mtok,
    p.cache_read_mtok,
    p.output_mtok,
    p.web_search_per_1k,
    LENGTH(p.model_prefix) AS prefix_length
  FROM
    `moz-fx-data-shared-prod.ai_usage_derived.claude_usage_v1` AS u
  LEFT JOIN
    pricing AS p
    ON STARTS_WITH(u.model, p.model_prefix)
  WHERE
    u.date = @submission_date
  QUALIFY
    prefix_length = MAX(prefix_length) OVER (PARTITION BY u.date, u.api_key_id, u.model)
)
SELECT
  date,
  api_key_id,
  model,
  CONCAT('claude-', REGEXP_EXTRACT(model, r'(opus|sonnet|haiku)')) AS model_family,
  CAST(
    ROUND(
      (uncached_input_tokens / 1e6 * input_mtok) + (
        cache_creation_5m_input_tokens / 1e6 * cache_write_5m_mtok
      ) + (cache_creation_1h_input_tokens / 1e6 * cache_write_1h_mtok) + (
        cache_read_input_tokens / 1e6 * cache_read_mtok
      ) + (output_tokens / 1e6 * output_mtok) + (web_search_requests / 1e3 * web_search_per_1k),
      6
    ) AS NUMERIC
  ) AS cost_usd
FROM
  usage_with_pricing
