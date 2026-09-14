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
  )
SELECT
  ti.corpus_item_id,
  bt.bucket,
  COUNTIF(e.event_name = 'click') AS clicks,
  COUNTIF(e.event_name = 'impression') AS impressions
FROM
  treatment_items ti
CROSS JOIN
  bucket_times bt
LEFT JOIN
  events e
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
    """Return treatment clicks and impressions for the last 144 buckets."""
    return client.query(TIMELINE_QUERY).to_dataframe()
