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
    dp.document_id,
    dp.submission_timestamp,
    dp.normalized_country_code,
    e.name AS event_name,
    mozfun.map.get_key(e.extra, 'scheduled_corpus_item_id') AS scheduled_corpus_item_id,
    mozfun.map.get_key(e.extra, 'corpus_item_id') AS corpus_item_id,
    TIMESTAMP_MILLIS(
      SAFE_CAST(mozfun.map.get_key(e.extra, 'recommended_at') AS INT64)
    ) AS recommended_at
  FROM
    deduplicated_pings dp
  CROSS JOIN
    UNNEST(dp.events) e
  WHERE
    -- Filter to Pocket events only
    e.category = 'pocket'
    AND e.name IN ('impression', 'click')
    -- Keep only data with a non-null scheduled_corpus_item_id or corpus_item_id
    AND (
      mozfun.map.get_key(e.extra, 'scheduled_corpus_item_id') IS NOT NULL
      OR mozfun.map.get_key(e.extra, 'corpus_item_id') IS NOT NULL
    )
    -- recommended_at must be parseable
    AND SAFE_CAST(mozfun.map.get_key(e.extra, 'recommended_at') AS INT64) IS NOT NULL
),
/* --------------------------------------------------------------------------
   Identify the single "canonical" corpus_item_id for each scheduled_corpus_item_id,
   if any event exists that associates it with its corpus_item_id.
   -------------------------------------------------------------------------- */
aggregator_scheduled_lookup AS (
  SELECT
    scheduled_corpus_item_id,
    -- If multiple rows have different corpus_item_id, pick the max.
    MAX(corpus_item_id) AS corpus_item_id
  FROM
    flattened_newtab_events
  WHERE
    recommended_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    AND scheduled_corpus_item_id IS NOT NULL
  GROUP BY
    scheduled_corpus_item_id
),
/* --------------------------------------------------------------------------
   Aggregate all events where scheduled_corpus_item_id is not null,
   and lookup corpus_item_id for those ids.
   -------------------------------------------------------------------------- */
aggregated_scheduled AS (
  SELECT
    fe.scheduled_corpus_item_id,
    fe.normalized_country_code,
    -- The lookup value may be non-null even if fe.corpus_item_id was null
    asl.corpus_item_id AS corpus_item_id,
    SUM(CASE WHEN fe.event_name = 'impression' THEN 1 ELSE 0 END) AS impression_count,
    SUM(CASE WHEN fe.event_name = 'click' THEN 1 ELSE 0 END) AS click_count
  FROM
    flattened_newtab_events fe
  LEFT JOIN
    aggregator_scheduled_lookup asl
    ON fe.scheduled_corpus_item_id = asl.scheduled_corpus_item_id
  WHERE
    fe.recommended_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    AND fe.scheduled_corpus_item_id IS NOT NULL
  GROUP BY
    fe.scheduled_corpus_item_id,
    fe.normalized_country_code,
    asl.corpus_item_id
),
/* --------------------------------------------------------------------------
   STEP C: Aggregate rows that have a NULL scheduled_corpus_item_id,
           i.e. items that were never “scheduled” for a date.
   -------------------------------------------------------------------------- */
aggregated_unscheduled AS (
  SELECT
    CAST(NULL AS STRING) AS scheduled_corpus_item_id,
    corpus_item_id,
    normalized_country_code,
    SUM(CASE WHEN event_name = 'impression' THEN 1 ELSE 0 END) AS impression_count,
    SUM(CASE WHEN event_name = 'click' THEN 1 ELSE 0 END) AS click_count
  FROM
    flattened_newtab_events
  WHERE
    recommended_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    AND scheduled_corpus_item_id IS NULL
  GROUP BY
    corpus_item_id,
    normalized_country_code
),
/* --------------------------------------------------------------------------
   STEP D: Union the two sets of aggregates (scheduled + unscheduled).
   -------------------------------------------------------------------------- */
aggregated_events AS (
  SELECT
    scheduled_corpus_item_id,
    corpus_item_id,
    normalized_country_code,
    impression_count,
    click_count
  FROM
    aggregated_scheduled
  UNION ALL
  SELECT
    scheduled_corpus_item_id,
    corpus_item_id,
    normalized_country_code,
    impression_count,
    click_count
  FROM
    aggregated_unscheduled
),
/* --------------------------------------------------------------------------
   STEP E: Global aggregates: sum impressions/clicks for each
           (scheduled_corpus_item_id, corpus_item_id) across *all* regions.
   -------------------------------------------------------------------------- */
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
/* --------------------------------------------------------------------------
   STEP F: "Regional" aggregates:
           impressions/clicks broken out by normalized_country_code
           for specific countries.
   -------------------------------------------------------------------------- */
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
    normalized_country_code IN ('US', 'CA', 'DE', 'CH', 'AT', 'BE', 'GB', 'IE')
)
/* --------------------------------------------------------------------------
   STEP G: Combine the "global" (no region) with the "regional" breakdown.
   -------------------------------------------------------------------------- */
SELECT
  *
FROM
  global_aggregates
UNION ALL
SELECT
  *
FROM
  country_aggregates;
