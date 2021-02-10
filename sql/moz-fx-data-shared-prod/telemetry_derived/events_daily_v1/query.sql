WITH sample AS (
  SELECT
    *
  FROM
    telemetry.deanonymized_events
),
events AS (
  SELECT
    *
  FROM
    sample
  WHERE
    submission_date = @submission_date
    OR (@submission_date IS NULL AND submission_date >= '2020-10-01')
),
joined AS (
  SELECT
    CONCAT(udf.pack_event_properties(events.extra, event_types.event_properties), index) AS index,
    events.* EXCEPT (category, event, extra)
  FROM
    events
  INNER JOIN
    telemetry.event_types event_types
  USING
    (category, event)
)
SELECT
  submission_date,
  client_id,
  sample_id,
  CONCAT(STRING_AGG(index, ',' ORDER BY timestamp ASC), ',') AS events,
  -- client info
  mozfun.stats.mode_last(ARRAY_AGG(application.build_id)) AS build_id,
  mozfun.stats.mode_last(ARRAY_AGG(environment.build.architecture)) AS build_architecture,
  mozfun.stats.mode_last(ARRAY_AGG(environment.profile.creation_date)) AS profile_creation_date,
  mozfun.stats.mode_last(ARRAY_AGG(environment.settings.is_default_browser)) AS is_default_browser,
  mozfun.stats.mode_last(ARRAY_AGG(environment.settings.attribution.source)) AS attribution_source,
  mozfun.stats.mode_last(ARRAY_AGG(metadata.uri.app_version)) AS app_version,
  mozfun.stats.mode_last(ARRAY_AGG(environment.settings.locale)) AS locale,
  mozfun.stats.mode_last(ARRAY_AGG(environment.partner.distribution_id)) AS distribution_id,
  mozfun.stats.mode_last(ARRAY_AGG(environment.settings.attribution.ua)) AS attribution_ua,
  mozfun.stats.mode_last(ARRAY_AGG(application.display_version)) AS display_version,
  -- metadata
  mozfun.stats.mode_last(ARRAY_AGG(metadata.geo.city)) AS city,
  mozfun.stats.mode_last(ARRAY_AGG(metadata.geo.country)) AS country,
  mozfun.stats.mode_last(ARRAY_AGG(metadata.geo.subdivision1)) AS subdivision1,
  -- normalized fields
  mozfun.stats.mode_last(ARRAY_AGG(normalized_channel)) AS channel,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_os)) AS os,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_os_version)) AS os_version,
  -- ping info
  mozfun.map.mode_last(ARRAY_CONCAT_AGG(experiments)) AS experiments
FROM
  joined
GROUP BY
  submission_date,
  client_id,
  sample_id
