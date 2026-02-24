WITH params AS (
  SELECT
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 15 DAY AS start_timestamp,
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 1 DAY AS end_timestamp,
    11 AS minutes_to_assume_random_ranking,
    "US" AS country,
    200 AS max_items_to_consider,
    20 AS per_item_cutoff
),
corpus_items AS (
  SELECT
    approved_corpus_item_external_id AS corpus_item_id,
    loaded_from,
    reviewed_corpus_item_created_at,
    LANGUAGE,
    topic
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.approved_corpus_items`,
    params
  WHERE
    LANGUAGE ="EN"
    AND reviewed_corpus_item_created_at >= params.start_timestamp
    AND reviewed_corpus_item_created_at <= params.end_timestamp
    AND is_time_sensitive IS FALSE
),
events AS (
  SELECT
    e.submission_timestamp,
    DATE(e.submission_timestamp) AS submission_date,
    metrics.string.newtab_content_experiment_branch AS branch,
    (SELECT ANY_VALUE(x.value) FROM UNNEST(ev.extra) AS x WHERE x.key = 'format') AS tile_format,
    CAST(
      (
        SELECT
          ANY_VALUE(x.value)
        FROM
          UNNEST(ev.extra) AS x
        WHERE
          x.key = 'section_position'
      ) AS INT64
    ) AS section_position,
    CAST(
      (SELECT ANY_VALUE(x.value) FROM UNNEST(ev.extra) AS x WHERE x.key = 'position') AS INT64
    ) AS position,
    (
      SELECT
        ANY_VALUE(x.value)
      FROM
        UNNEST(ev.extra) AS x
      WHERE
        x.key = 'corpus_item_id'
    ) AS corpus_item_id,
    ev.name AS event_name
  FROM
    params,
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_content_v1` AS e
  CROSS JOIN
    UNNEST(e.events) AS ev
  WHERE
    e.submission_timestamp
    BETWEEN params.start_timestamp
    AND params.end_timestamp
    AND ev.name IN ('impression', 'click')
    AND (
      metrics.string.newtab_content_experiment_name = ""
      OR metrics.string.newtab_content_experiment_name IS NULL
    )
    AND metrics.string.newtab_content_country = params.country
),
base_events_fresh_items AS (
  SELECT
    submission_date,
    branch,
    tile_format,
    section_position,
    position,
    corpus_item_id,
    event_name
  FROM
    events ev,
    params
  WHERE
    EXISTS(
      SELECT
        1
      FROM
        corpus_items ci
      WHERE
        ci.corpus_item_id = ev.corpus_item_id
        AND ev.submission_timestamp < TIMESTAMP_ADD(
          ci.reviewed_corpus_item_created_at,
          INTERVAL params.minutes_to_assume_random_ranking MINUTE
        )
        AND ev.section_position IS NOT NULL
    )
),
aggregates AS (
  SELECT
    tile_format,
    section_position,
    position,
    COUNTIF(event_name = 'impression') AS impressions,
    COUNTIF(event_name = 'click') AS clicks
  FROM
    base_events_fresh_items
  GROUP BY
    tile_format,
    section_position,
    position
),
stories_aggregates AS (
  SELECT
    position,
    tile_format,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr
  FROM
    aggregates,
    params
  WHERE
    position >= 0
    AND position <= params.max_items_to_consider
  GROUP BY
    position,
    tile_format
),
long_tail_aggregates AS (
  -- pooled/averaged over larger positions, by tile_format only
  SELECT
    tile_format,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr
  FROM
    aggregates,
    params
  WHERE
    position > params.per_item_cutoff
    AND position <= params.max_items_to_consider
  GROUP BY
    tile_format
),
stories_totals AS (
  SELECT
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr
  FROM
    stories_aggregates
),
stories_weights AS (
  SELECT
    SAFE_DIVIDE(stories_totals.ctr, NULLIF(ag.ctr, 0)) AS unormalized_weight,
    ag.impressions,
    ag.position,
    ag.tile_format
  FROM
    stories_totals,
    stories_aggregates AS ag
),
long_tail_weights AS (
  SELECT
    SAFE_DIVIDE(stories_totals.ctr, NULLIF(ag.ctr, 0)) AS unormalized_weight,
    ag.impressions,
    ag.tile_format
  FROM
    stories_totals,
    long_tail_aggregates AS ag
),
-- collect all formats we might want to output weights for
all_formats AS (
  SELECT DISTINCT
    tile_format
  FROM
    stories_aggregates
  WHERE
    tile_format IS NOT NULL
),
-- generate the long-tail positions we want to "fill" with the averaged-by-format weight
long_tail_positions AS (
  SELECT
    pos AS position
  FROM
    params,
    UNNEST(GENERATE_ARRAY(params.per_item_cutoff + 1, params.max_items_to_consider)) AS pos
),
merged_weights AS (
  -- first per_item_cutoff items: keep per-position weights
  SELECT
    unormalized_weight,
    impressions,
    position,
    tile_format
  FROM
    stories_weights,
    params
  WHERE
    position
    BETWEEN 0
    AND params.per_item_cutoff
  UNION ALL
  -- long_tail_weights (averaged by tile_format), replicated across positions > cutoff,
  -- and include all possible format values (even if long_tail_weights is missing for a format)
  SELECT
    lt.unormalized_weight,
    lt.impressions,
    p.position,
    f.tile_format
  FROM
    all_formats f
  CROSS JOIN
    long_tail_positions p
  LEFT JOIN
    long_tail_weights lt
    ON lt.tile_format = f.tile_format
),
-- Now that we have weights, normalize (scale) all of them so overall average CTR is unchanged.
base_events_all_items AS (
  SELECT
    tile_format,
    position,
    event_name
  FROM
    events ev,
    params
  WHERE
    ev.section_position IS NOT NULL
    AND ev.position IS NOT NULL
    AND ev.position >= 0
    AND ev.position <= params.max_items_to_consider
),
aggregates_all_items AS (
  SELECT
    tile_format,
    position,
    COUNTIF(event_name = 'impression') AS impressions,
    COUNTIF(event_name = 'click') AS clicks
  FROM
    base_events_all_items
  GROUP BY
    tile_format,
    position
),
adjust_sums AS (
  SELECT
    SUM(
      SAFE_DIVIDE(aggregates_all_items.impressions, NULLIF(merged_weights.unormalized_weight, 0))
    ) AS denom_sum,
    SUM(aggregates_all_items.clicks) AS clicks_sum
  FROM
    aggregates_all_items
  JOIN
    merged_weights
    ON merged_weights.position = aggregates_all_items.position
    AND merged_weights.tile_format = aggregates_all_items.tile_format
),
all_items_stats AS (
  SELECT
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS target_ctr
  FROM
    aggregates_all_items
),
normalization_factor AS (
  SELECT
    SAFE_DIVIDE(
      all_items_stats.target_ctr * adjust_sums.denom_sum,
      NULLIF(adjust_sums.clicks_sum, 0)
    ) AS factor
  FROM
    all_items_stats,
    adjust_sums
),
normalized_weights AS (
  SELECT
    merged_weights.unormalized_weight * COALESCE(normalization_factor.factor, 1.0) AS weight,
    merged_weights.position,
    merged_weights.tile_format,
    NULL AS section_position,
    merged_weights.impressions
  FROM
    merged_weights
  CROSS JOIN
    normalization_factor
  WHERE
    merged_weights.impressions > 2000
    AND merged_weights.unormalized_weight IS NOT NULL
    AND merged_weights.unormalized_weight != 0
)
SELECT
  *
FROM
  normalized_weights
UNION ALL
-- 'any' format: impression-weighted average propensity across all formats per position
SELECT
  SAFE_DIVIDE(SUM(weight * impressions), SUM(impressions)) AS weight,
  position,
  'any' AS tile_format,
  NULL AS section_position,
  SUM(impressions) AS impressions
FROM
  normalized_weights
GROUP BY
  position;
