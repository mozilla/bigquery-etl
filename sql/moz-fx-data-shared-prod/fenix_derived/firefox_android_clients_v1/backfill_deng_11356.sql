-- Sidefill for DENG-11356: populate the play_store_attribution_* fields for clients first seen
-- from the 2026-04-06 ping cutover onwards, from the dedicated `play_store_attribution` ping.
--
-- `firefox_android_clients_v1` has depends_on_past: true with a null date_partition_parameter, so
-- it can't be backfilled one partition at a time via query.sql. Rather than reinitialize the whole
-- table (which would rewrite every partition back to 2020), this reads each first_seen_date
-- partition's own production data and only replaces the six play_store_attribution_* fields and
-- their metadata ping datetimes. It is therefore partition-independent, as the backfill code
-- requires for `override_depends_on_past_null_partition`.
--
-- The ping is matched on the client's first-seen day, which is where 23,489 of the 23,516
-- attributed clients in the validation week land. The ping preference and the COALESCE fallback
-- to any pre-existing value match the updated query.sql exactly.
WITH play_store_attribution_ping AS (
  SELECT
    client_info.client_id AS client_id,
    ARRAY_AGG(
      IF(
        NULLIF(metrics.string.play_store_attribution_campaign, "") IS NULL,
        NULL,
        STRUCT(
          NULLIF(metrics.string.play_store_attribution_campaign, "") AS value,
          DATETIME(submission_timestamp) AS ping_datetime
        )
      ) IGNORE NULLS
      ORDER BY
        ping_info.seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS play_store_attribution_campaign,
    ARRAY_AGG(
      IF(
        NULLIF(metrics.string.play_store_attribution_content, "") IS NULL,
        NULL,
        STRUCT(
          NULLIF(metrics.string.play_store_attribution_content, "") AS value,
          DATETIME(submission_timestamp) AS ping_datetime
        )
      ) IGNORE NULLS
      ORDER BY
        ping_info.seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS play_store_attribution_content,
    ARRAY_AGG(
      IF(
        NULLIF(metrics.string.play_store_attribution_medium, "") IS NULL,
        NULL,
        STRUCT(
          NULLIF(metrics.string.play_store_attribution_medium, "") AS value,
          DATETIME(submission_timestamp) AS ping_datetime
        )
      ) IGNORE NULLS
      ORDER BY
        ping_info.seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS play_store_attribution_medium,
    ARRAY_AGG(
      IF(
        NULLIF(metrics.string.play_store_attribution_source, "") IS NULL,
        NULL,
        STRUCT(
          NULLIF(metrics.string.play_store_attribution_source, "") AS value,
          DATETIME(submission_timestamp) AS ping_datetime
        )
      ) IGNORE NULLS
      ORDER BY
        ping_info.seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS play_store_attribution_source,
    ARRAY_AGG(
      IF(
        NULLIF(metrics.string.play_store_attribution_term, "") IS NULL,
        NULL,
        STRUCT(
          NULLIF(metrics.string.play_store_attribution_term, "") AS value,
          DATETIME(submission_timestamp) AS ping_datetime
        )
      ) IGNORE NULLS
      ORDER BY
        ping_info.seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS play_store_attribution_term,
    ARRAY_AGG(
      IF(
        NULLIF(metrics.text2.play_store_attribution_install_referrer_response, "") IS NULL,
        NULL,
        STRUCT(
          NULLIF(metrics.text2.play_store_attribution_install_referrer_response, "") AS value,
          DATETIME(submission_timestamp) AS ping_datetime
        )
      ) IGNORE NULLS
      ORDER BY
        ping_info.seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS play_store_attribution_install_referrer_response,
  FROM
    `moz-fx-data-shared-prod.fenix.play_store_attribution`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND DATE(submission_timestamp) >= "2026-04-06"
    AND client_info.client_id IS NOT NULL
  GROUP BY
    client_id
)
SELECT
  clients.* REPLACE (
    COALESCE(
      play_store.play_store_attribution_campaign.value,
      clients.play_store_attribution_campaign
    ) AS play_store_attribution_campaign,
    COALESCE(
      play_store.play_store_attribution_content.value,
      clients.play_store_attribution_content
    ) AS play_store_attribution_content,
    COALESCE(
      play_store.play_store_attribution_medium.value,
      clients.play_store_attribution_medium
    ) AS play_store_attribution_medium,
    COALESCE(
      play_store.play_store_attribution_source.value,
      clients.play_store_attribution_source
    ) AS play_store_attribution_source,
    COALESCE(
      play_store.play_store_attribution_term.value,
      clients.play_store_attribution_term
    ) AS play_store_attribution_term,
    COALESCE(
      play_store.play_store_attribution_install_referrer_response.value,
      clients.play_store_attribution_install_referrer_response
    ) AS play_store_attribution_install_referrer_response,
    STRUCT(
      clients.metadata.reported_first_session_ping AS reported_first_session_ping,
      clients.metadata.reported_metrics_ping AS reported_metrics_ping,
      clients.metadata.reported_baseline_ping AS reported_baseline_ping,
      clients.metadata.min_first_session_ping_submission_date AS min_first_session_ping_submission_date,
      clients.metadata.min_first_session_ping_run_date AS min_first_session_ping_run_date,
      clients.metadata.min_metrics_ping_submission_date AS min_metrics_ping_submission_date,
      clients.metadata.adjust_network__source_ping AS adjust_network__source_ping,
      clients.metadata.install_source__source_ping AS install_source__source_ping,
      clients.metadata.adjust_network__source_ping_datetime AS adjust_network__source_ping_datetime,
      clients.metadata.install_source__source_ping_datetime AS install_source__source_ping_datetime,
      CASE
        WHEN play_store.play_store_attribution_campaign.value IS NOT NULL
          THEN play_store.play_store_attribution_campaign.ping_datetime
        ELSE clients.metadata.play_store_attribution_campaign__ping_datetime
      END AS play_store_attribution_campaign__ping_datetime,
      CASE
        WHEN play_store.play_store_attribution_content.value IS NOT NULL
          THEN play_store.play_store_attribution_content.ping_datetime
        ELSE clients.metadata.play_store_attribution_content__ping_datetime
      END AS play_store_attribution_content__ping_datetime,
      CASE
        WHEN play_store.play_store_attribution_medium.value IS NOT NULL
          THEN play_store.play_store_attribution_medium.ping_datetime
        ELSE clients.metadata.play_store_attribution_medium__ping_datetime
      END AS play_store_attribution_medium__ping_datetime,
      CASE
        WHEN play_store.play_store_attribution_source.value IS NOT NULL
          THEN play_store.play_store_attribution_source.ping_datetime
        ELSE clients.metadata.play_store_attribution_source__ping_datetime
      END AS play_store_attribution_source__ping_datetime,
      CASE
        WHEN play_store.play_store_attribution_term.value IS NOT NULL
          THEN play_store.play_store_attribution_term.ping_datetime
        ELSE clients.metadata.play_store_attribution_term__ping_datetime
      END AS play_store_attribution_term__ping_datetime,
      CASE
        WHEN play_store.play_store_attribution_install_referrer_response.value IS NOT NULL
          THEN play_store.play_store_attribution_install_referrer_response.ping_datetime
        ELSE clients.metadata.play_store_attribution_install_referrer_response__ping_datetime
      END AS play_store_attribution_install_referrer_response__ping_datetime,
      clients.metadata.meta_attribution_app__ping_datetime AS meta_attribution_app__ping_datetime
    ) AS metadata
  )
FROM
  `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1` AS clients
LEFT JOIN
  play_store_attribution_ping AS play_store
  USING (client_id)
WHERE
  clients.first_seen_date = @submission_date
