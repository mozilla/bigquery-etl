WITH counts AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    JSON_VALUE(engine.name) AS engine_name,
    CASE
      WHEN normalized_channel IN ("aurora", "beta")
        THEN "beta"
      ELSE normalized_channel
    END AS channel,
    COUNTIF(syncs.failureReason IS NOT NULL) AS count_sync_errors,
    COUNTIF(engine.failureReason IS NOT NULL) AS count_engine_errors,
    COUNTIF(
      syncs.failureReason IS NOT NULL
      OR engine.failureReason IS NOT NULL
    ) AS count_total_errors,
    COUNTIF(
      syncs.failureReason IS NULL
      AND engine.failureReason IS NULL
      AND (engine.incoming IS NOT NULL OR engine.outgoing IS NOT NULL OR engine.took IS NOT NULL)
    ) AS count_success,
    AVG(INT64(engine.took)) / 1000.0 AS avg_sync_time,
    IFNULL(SUM(INT64(engine.incoming.applied)), 0) AS incoming_applied,
    IFNULL(SUM(INT64(engine.incoming.failed)), 0) AS incoming_failed,
    IFNULL(
      SUM(
        (
          SELECT
            SUM(INT64(outgoing.sent))
          FROM
            UNNEST(JSON_QUERY_ARRAY(engine.outgoing)) AS outgoing
        )
      ),
      0
    ) AS outgoing_sent,
    IFNULL(
      SUM(
        (
          SELECT
            SUM(INT64(outgoing.failed))
          FROM
            UNNEST(JSON_QUERY_ARRAY(engine.outgoing)) AS outgoing
        )
      ),
      0
    ) AS outgoing_failed,
    COUNT(*) AS count_total,
  FROM
    mozdata.firefox_desktop.sync
  CROSS JOIN
    UNNEST(JSON_QUERY_ARRAY(metrics.object.syncs_syncs)) AS syncs
  CROSS JOIN
    UNNEST(JSON_QUERY_ARRAY(syncs, '$.engines')) AS engine
  WHERE
    metrics IS NOT NULL
    AND JSON_VALUE(engine.name) NOT IN ("bookmarks", "extension-storage")
  GROUP BY
    submission_date,
    engine_name,
    channel
)
SELECT
  "desktop" AS application,
  channel,
  engine_name,
  submission_date,
  count_sync_errors,
  count_engine_errors,
  count_total_errors,
  count_success,
  avg_sync_time,
  incoming_applied,
  incoming_failed,
  outgoing_sent,
  outgoing_failed,
  count_total,
  count_total_errors / (count_success + count_total_errors) * 100 AS error_rate,
  count_success / (count_success + count_total_errors) * 100 AS success_rate,
  incoming_applied + incoming_failed + outgoing_sent + outgoing_failed AS total_record_count
FROM
  counts
WHERE
  submission_date = @submission_date
