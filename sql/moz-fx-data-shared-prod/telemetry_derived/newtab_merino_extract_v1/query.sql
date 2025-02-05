WITH deduplicated_pings AS (
  SELECT
    submission_timestamp,
    document_id,
    events,
    normalized_country_code
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
  WHERE
    submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        DATE(submission_timestamp),
        document_id
      ORDER BY
        submission_timestamp DESC
    ) = 1
),
flattened_newtab_events AS (
  SELECT
    document_id,
    submission_timestamp,
    normalized_country_code,
    unnested_events.name AS event_name,
    mozfun.map.get_key(
      unnested_events.extra,
      'scheduled_corpus_item_id'
    ) AS scheduled_corpus_item_id,
    mozfun.map.get_key(unnested_events.extra, 'corpus_item_id') AS corpus_item_id,
    TIMESTAMP_MILLIS(
      SAFE_CAST(mozfun.map.get_key(unnested_events.extra, 'recommended_at') AS INT64)
    ) AS recommended_at
  FROM
    deduplicated_pings dp
  CROSS JOIN
    UNNEST(dp.events) AS unnested_events
  WHERE
    -- Filter to Pocket events only
    unnested_events.category = 'pocket'
    AND unnested_events.name IN ('impression', 'click')
    -- Keep only data with a non-null scheduled_corpus_item_id or corpus_item_id
    AND (
      mozfun.map.get_key(unnested_events.extra, 'scheduled_corpus_item_id') IS NOT NULL
      OR mozfun.map.get_key(unnested_events.extra, 'corpus_item_id') IS NOT NULL
    )
    -- Only keep the last day's data.
    AND TIMESTAMP_MILLIS(
      SAFE_CAST(mozfun.map.get_key(unnested_events.extra, 'recommended_at') AS INT64)
    ) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
),
/* Map scheduled_corpus_item_id to corpus_item_id.
 * In the backend all scheduled_corpus_item_id with the same value map to the same corpus_item_id.
 * In these events some corpus_item_id are null because it is only emitted by Firefox >= 134.
 */
aggregator_scheduled_lookup AS (
  SELECT
    scheduled_corpus_item_id,
    -- Use MAX to get a non-null corpus_item_id if one exists. All non-null values are the same.
    MAX(corpus_item_id) AS corpus_item_id
  FROM
    flattened_newtab_events
  WHERE
    scheduled_corpus_item_id IS NOT NULL
  GROUP BY
    scheduled_corpus_item_id
),
/* Aggregate clicks and impressions by scheduled_corpus_item_id, corpus_item_id, normalized_country_code. */
aggregated_events AS (
  SELECT
    fe.scheduled_corpus_item_id,
    -- fe.corpus_item_id can be NULL because it's only emitted by Firefox >= 134.
    COALESCE(fe.corpus_item_id, asl.corpus_item_id) AS corpus_item_id,
    fe.normalized_country_code,
    SUM(CASE WHEN fe.event_name = 'impression' THEN 1 ELSE 0 END) AS impression_count,
    SUM(CASE WHEN fe.event_name = 'click' THEN 1 ELSE 0 END) AS click_count
  FROM
    flattened_newtab_events fe
  LEFT JOIN
    aggregator_scheduled_lookup asl
    ON fe.scheduled_corpus_item_id = asl.scheduled_corpus_item_id
  GROUP BY
    1,
    2,
    3
),
/* Aggregate clicks and impressions across all countries. */
global_aggregates AS (
  SELECT
    scheduled_corpus_item_id,
    corpus_item_id,
    CAST(NULL AS STRING) AS region,
    SUM(impression_count) AS impression_count,
    SUM(click_count) AS click_count
  FROM
    aggregated_events
  GROUP BY
    scheduled_corpus_item_id,
    corpus_item_id
),
/* Aggregate clicks and impressions for country-specific ranking in Merino. */
country_aggregates AS (
  SELECT
    scheduled_corpus_item_id,
    corpus_item_id,
    normalized_country_code AS region,
    impression_count,
    click_count
  FROM
    aggregated_events
  WHERE
    -- Gather country (a.k.a. region) specific engagement for all countries that share a feed.
    -- https://mozilla-hub.atlassian.net/wiki/x/JY3LB
    normalized_country_code IN ('US', 'CA', 'DE', 'CH', 'AT', 'BE', 'GB', 'IE')
)
/* Combine the "global" (no region) with the "regional" breakdown. */
SELECT
  *
FROM
  global_aggregates
UNION ALL
SELECT
  *
FROM
  country_aggregates;
