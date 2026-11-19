-- Fill attribution_msstoresignedin for historical first-seen rows. Run per
-- first_seen_date partition (@submission_date); other columns carried via `* REPLACE`.
-- Join each row to its contributing ping (metadata.first_seen_date_source_ping),
-- picking the earliest ping per source to match prod (not ANY_VALUE). Graft only when
-- NULL and the other 9 attribution fields still match the row (drift guard).
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
    -- clients_daily_v6 is unique per (client_id, submission_date); the filter
    -- leaves one row per client (earliest-date offset(0)).
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
    -- Fill only when NULL; never overwrite. Require the ping to exist and its
    -- other 9 attribution fields to match the row (NULL-safe), so the grafted
    -- value belongs to the attribution prod stored.
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
