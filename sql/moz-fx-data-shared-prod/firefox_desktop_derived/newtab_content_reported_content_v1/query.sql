WITH newtab_content_live_deduped AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_content_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        DATE(submission_timestamp),
        document_id
      ORDER BY
        submission_timestamp
    ) = 1
),
newtab_content_live_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    mozfun.map.get_key(event.extra, 'card_type') AS card_type,
    mozfun.map.get_key(event.extra, 'corpus_item_id') AS corpus_item_id,
    mozfun.map.get_key(event.extra, 'report_reason') AS report_reason,
    mozfun.map.get_key(event.extra, 'section') AS section,
    mozfun.map.get_key(event.extra, 'section_position') AS section_position,
    mozfun.map.get_key(event.extra, 'title') AS title,
    mozfun.map.get_key(event.extra, 'topic') AS topic,
    mozfun.map.get_key(event.extra, 'url') AS url
  FROM
    newtab_content_live_deduped AS e
  CROSS JOIN
    UNNEST(e.events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event.category = 'newtab_content'
    AND event.name = 'report_content_submit'
)
SELECT
  *
FROM
  newtab_content_live_events
