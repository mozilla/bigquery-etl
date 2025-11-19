WITH params AS (
  SELECT
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 8 DAY AS start_timestamp,
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 1 DAY AS end_timestamp,
    11 AS minutes_to_assume_random_ranking,
    "US" AS country
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
    SUM(clicks) / SUM(impressions) AS ctr
  FROM
    aggregates
  WHERE
    position >= 0
    AND position <= 50
  GROUP BY
    position,
    tile_format
),
stories_totals AS (
  SELECT
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(clicks) / SUM(impressions) AS ctr
  FROM
    stories_aggregates
),
stories_weights AS (
  SELECT
    SAFE_DIVIDE(stories_totals.ctr, ag.ctr) AS unormalized_weight,
    ag.impressions,
    position,
    tile_format
  FROM
    stories_totals,
    stories_aggregates AS ag
),
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
adjusted_all_data_clicks AS (
  SELECT
    aggregates_all_items.clicks / stories_weights.unormalized_weight as clicks_adjusted,
    aggregates_all_items.position,
    aggregates_all_items.tile_format
  FROM
    aggregates_all_items JOIN stories_weights ON (stories_weights.position = aggregates_all_items.position AND  
          stories_weights.tile_format = aggregates_all_items.tile_format)
),
all_items_stats AS (
  SELECT SUM(impressions) as impressions,
   SUM(clicks) as clicks,
   SAFE_DIVIDE(SUM(clicks), SUM(impressions)) as target_ctr
   from aggregates_all_items
),
totals_all_items AS (
  SELECT SUM(impressions) AS impressions_all
  FROM aggregates_all_items
),
adjusted_clicks_total AS (
  SELECT SUM(clicks_adjusted) AS clicks_adj_total
  FROM adjusted_all_data_clicks
),
normalization_factor AS (
  SELECT
    SAFE_DIVIDE(all_items_stats.target_ctr * totals_all_items.impressions_all,
                adjusted_clicks_total.clicks_adj_total) AS factor
  FROM all_items_stats, totals_all_items, adjusted_clicks_total
)
SELECT
  unormalized_weight * normalization_factor.factor as weight,
  position,
  tile_format,
  NULL AS section_position,
  impressions
FROM
  stories_weights
CROSS JOIN normalization_factor
WHERE impressions > 2000
