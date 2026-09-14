"""Read the treatment timeline used by the CTR state-space model."""


TIMELINE_QUERY = """
WITH
  bounds AS (
    SELECT
      TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(CURRENT_TIMESTAMP()), 600) * 600) AS end_time
  ),
  treatment_items AS (
    SELECT DISTINCT
      corpus_item_id
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.newtab_merino_extract_v3`
    WHERE
      region = 'GB-ctrpred_engb-treatment'
  ),
  bucket_times AS (
    SELECT
      bucket,
      TIMESTAMP_ADD(
        TIMESTAMP_SUB(bounds.end_time, INTERVAL 24 HOUR),
        INTERVAL (bucket * 10) MINUTE
      ) AS bucket_time
    FROM
      bounds
    CROSS JOIN
      UNNEST(GENERATE_ARRAY(0, 143)) AS bucket
  ),
  propensity_weights AS (
    SELECT
      country,
      position,
      tile_format,
      weight
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.newtab_merino_propensity_v2`
    WHERE
      layout = 'SECTION_GRID'
      AND section_position IS NULL
      AND snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    QUALIFY
      snapshot_date = MAX(snapshot_date) OVER ()
  ),
  pings AS (
    SELECT
      submission_timestamp,
      document_id,
      events
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_live.newtab_content_v1`
    CROSS JOIN
      bounds
    WHERE
      submission_timestamp >= TIMESTAMP_SUB(bounds.end_time, INTERVAL 24 HOUR)
      AND submission_timestamp < bounds.end_time
      AND mozfun.newtab.surface_id_country(
        metrics.string.newtab_content_surface_id,
        NULL,
        metrics.string.newtab_content_country
      ) = 'GB'
      AND metrics.string.newtab_content_experiment_name = 'ctrpred_engb'
      AND metrics.string.newtab_content_experiment_branch = 'treatment'
  ),
  deduplicated_pings AS (
    SELECT
      *
    FROM
      pings
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          DATE(submission_timestamp),
          document_id
        ORDER BY
          submission_timestamp DESC
      ) = 1
  ),
  events AS (
    SELECT
      mozfun.map.get_key(event.extra, 'corpus_item_id') AS corpus_item_id,
      DIV(
        TIMESTAMP_DIFF(
          TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(dp.submission_timestamp), 600) * 600),
          TIMESTAMP_SUB(b.end_time, INTERVAL 24 HOUR),
          MINUTE
        ),
        10
      ) AS bucket,
      SAFE_CAST(mozfun.map.get_key(event.extra, 'position') AS INT64) AS position,
      mozfun.map.get_key(event.extra, 'format') AS tile_format,
      SAFE_CAST(
        mozfun.map.get_key(event.extra, 'section_position') AS INT64
      ) AS section_position,
      event.name AS event_name
    FROM
      deduplicated_pings dp
    CROSS JOIN
      bounds b
    CROSS JOIN
      UNNEST(dp.events) AS event
    INNER JOIN
      treatment_items
    ON
      treatment_items.corpus_item_id = mozfun.map.get_key(event.extra, 'corpus_item_id')
    WHERE
      event.category IN ('pocket', 'newtab_content')
      AND event.name IN ('impression', 'click')
  ),
  weighted_events AS (
    SELECT
      e.corpus_item_id,
      e.bucket,
      e.event_name,
      IF(
        e.section_position IS NULL,
        1.0,
        1.0 / COALESCE(
          wt_country_exact.weight,
          wt_global_exact.weight,
          wt_country_any.weight,
          wt_global_any.weight,
          1.0
        )
      ) AS impression_weight
    FROM
      events e
    LEFT JOIN
      propensity_weights wt_country_exact
    ON
      wt_country_exact.country = 'GB'
      AND SAFE_CAST(wt_country_exact.position AS INT64) = e.position
      AND wt_country_exact.tile_format = e.tile_format
    LEFT JOIN
      propensity_weights wt_country_any
    ON
      wt_country_any.country = 'GB'
      AND SAFE_CAST(wt_country_any.position AS INT64) = e.position
      AND wt_country_any.tile_format = 'any'
    LEFT JOIN
      propensity_weights wt_global_exact
    ON
      wt_global_exact.country IS NULL
      AND SAFE_CAST(wt_global_exact.position AS INT64) = e.position
      AND wt_global_exact.tile_format = e.tile_format
    LEFT JOIN
      propensity_weights wt_global_any
    ON
      wt_global_any.country IS NULL
      AND SAFE_CAST(wt_global_any.position AS INT64) = e.position
      AND wt_global_any.tile_format = 'any'
  )
SELECT
  ti.corpus_item_id,
  bt.bucket,
  COUNTIF(e.event_name = 'click') AS clicks,
  SUM(IF(e.event_name = 'impression', e.impression_weight, 0.0))
    AS adjusted_impressions
FROM
  treatment_items ti
CROSS JOIN
  bucket_times bt
LEFT JOIN
  weighted_events e
ON
  e.corpus_item_id = ti.corpus_item_id
  AND e.bucket = bt.bucket
GROUP BY
  ti.corpus_item_id,
  bt.bucket
ORDER BY
  ti.corpus_item_id,
  bt.bucket
"""


def query_timeline_data(client):
    """Return treatment clicks and adjusted impressions for 144 buckets."""
    return client.query(TIMELINE_QUERY).to_dataframe()
