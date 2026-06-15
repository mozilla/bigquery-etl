-- populate the newly added top-level column `attribution_msstoresignedin` for historical first-seen rows.
--
-- Only `attribution_msstoresignedin` is written; every other column is carried through
-- from the current production partition via `* REPLACE`. Run per first_seen_date
-- partition, with @submission_date bound to that partition's date.
--
-- Approach: join each first-seen row to the ping that contributed it. The contributing
-- ping type is stored on the row as metadata.first_seen_date_source_ping, and its date
-- is the row's first_seen_date (= @submission_date). Production builds every attribution_*
-- field (including msstoresignedin) from the SAME earliest ping per source
-- (ARRAY_AGG ... RESPECT NULLS ORDER BY <ts> [SAFE_OFFSET(0)]), so we replicate that exact
-- earliest-ping selection here rather than ANY_VALUE. We graft msstoresignedin only when
-- the other 9 attribution fields recomputed from that ping still match what is stored on
-- the row -- guarding against raw-ping drift (shredder deletions / late data) since the
-- row was originally computed.
WITH source_msstore AS (
  SELECT
    client_id,
    'new_profile' AS source_ping,
    ARRAY_AGG(
      STRUCT(
        environment.settings.attribution.campaign,
        environment.settings.attribution.content,
        environment.settings.attribution.dltoken,
        environment.settings.attribution.dlsource,
        environment.settings.attribution.experiment,
        environment.settings.attribution.medium,
        environment.settings.attribution.source,
        environment.settings.attribution.ua,
        environment.settings.attribution.variation,
        environment.settings.attribution.msstoresignedin
      )
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS attribution
  FROM
    `moz-fx-data-shared-prod.telemetry.new_profile`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
  UNION ALL
  SELECT
    client_id,
    'shutdown' AS source_ping,
    ARRAY_AGG(
      STRUCT(
        environment.settings.attribution.campaign,
        environment.settings.attribution.content,
        environment.settings.attribution.dltoken,
        environment.settings.attribution.dlsource,
        environment.settings.attribution.experiment,
        environment.settings.attribution.medium,
        environment.settings.attribution.source,
        environment.settings.attribution.ua,
        environment.settings.attribution.variation,
        environment.settings.attribution.msstoresignedin
      )
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS attribution
  FROM
    `moz-fx-data-shared-prod.telemetry.first_shutdown`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
  UNION ALL
  SELECT
    client_id,
    'main' AS source_ping,
    -- clients_daily_v6 is unique per (client_id, submission_date); the partition filter
    -- leaves one row per client, mirroring main_ping_agg's earliest-date offset(0).
    ARRAY_AGG(
      STRUCT(
        attribution.campaign,
        attribution.content,
        attribution.dltoken,
        attribution.dlsource,
        attribution.experiment,
        attribution.medium,
        attribution.source,
        attribution.ua,
        attribution.variation,
        attribution.msstoresignedin
      )
      ORDER BY
        submission_date
    )[SAFE_OFFSET(0)] AS attribution
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
)
SELECT
  cfs.* REPLACE (
    -- Only fill when the existing value is NULL; never overwrite an existing value.
    -- Also require the contributing ping to exist on this date AND its other 9
    -- attribution fields to still match what is stored on the row (NULL-safe), so the
    -- grafted msstoresignedin belongs to the same attribution prod actually stored.
    IF(
      cfs.attribution_msstoresignedin IS NULL
      AND s.client_id IS NOT NULL
      AND TO_JSON_STRING(
        (SELECT AS STRUCT s.attribution.* EXCEPT (msstoresignedin))
      ) = TO_JSON_STRING(
        STRUCT(
          cfs.attribution_campaign AS campaign,
          cfs.attribution_content AS content,
          cfs.attribution_dltoken AS dltoken,
          cfs.attribution_dlsource AS dlsource,
          cfs.attribution_experiment AS experiment,
          cfs.attribution_medium AS medium,
          cfs.attribution_source AS source,
          cfs.attribution_ua AS ua,
          cfs.attribution_variation AS variation
        )
      ),
      s.attribution.msstoresignedin,
      cfs.attribution_msstoresignedin
    ) AS attribution_msstoresignedin
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v3` AS cfs
LEFT JOIN
  source_msstore AS s
  ON cfs.client_id = s.client_id
  AND s.source_ping = cfs.metadata.first_seen_date_source_ping
WHERE
  cfs.first_seen_date = @submission_date
