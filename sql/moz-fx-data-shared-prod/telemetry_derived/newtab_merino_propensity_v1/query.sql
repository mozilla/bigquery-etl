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
base_events AS (
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
    base_events
  GROUP BY
    tile_format,
    section_position,
    position
),
top_stories_aggregates AS (
  SELECT
    position,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(clicks) / SUM(impressions) AS ctr
  FROM
    aggregates
  WHERE
    section_position = 0
    AND position >= 0
    AND position <= 7
  GROUP BY
    position
),
top_stories_totals AS (
  SELECT
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(clicks) / SUM(impressions) AS ctr
  FROM
    top_stories_aggregates
),
per_section_aggregates AS (
  SELECT
    section_position,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr
  FROM
    aggregates
  WHERE
    section_position <= 10
  GROUP BY
    section_position
),
per_section_totals AS (
  SELECT
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr
  FROM
    per_section_aggregates
),
top_stories_weights AS (
  SELECT
    position,
    SAFE_DIVIDE(top_stories_totals.ctr, ag.ctr) AS weight,
    ag.impressions
  FROM
    top_stories_totals,
    top_stories_aggregates AS ag
),
section_weights AS (
  SELECT
    section_position,
    SAFE_DIVIDE(per_section_totals.ctr, ag.ctr) AS weight,
    ag.impressions
  FROM
    per_section_aggregates AS ag,
    per_section_totals
)
SELECT
  weight,
  position,
  NULL AS section_position,
  impressions
FROM
  top_stories_weights
UNION ALL
SELECT
  weight,
  NULL AS position,
  section_position,
  impressions
FROM
  section_weights
