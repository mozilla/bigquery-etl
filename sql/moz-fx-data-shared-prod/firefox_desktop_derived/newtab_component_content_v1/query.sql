-- get the window width by visit, which is needed for rownumber
WITH visit_window_width AS (
  SELECT
    MIN(
      IF(
        category = 'newtab'
        AND name = "opened",
        SAFE_CAST(mozfun.map.get_key(extra, "window_inner_width") AS INT),
        NULL
      )
    ) AS newtab_window_inner_width,
    mozfun.map.get_key(extra, 'newtab_visit_id') AS newtab_visit_id
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    newtab_visit_id
),
-- unnest pocket events
pocket_events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    sample_id,
    client_info.client_id AS client_id,
    mozfun.map.get_key(extra, 'newtab_visit_id') AS newtab_visit_id,
    SAFE_CAST(
      mozfun.norm.browser_version_info(client_info.app_display_version).major_version AS INT64
    ) AS app_version,
    normalized_country_code AS country,
    name AS event_name,
    extra AS event_details,
    ping_info
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND category = 'pocket'
)
-- aggregate metrics to the visit_id level
SELECT
  submission_date,
  sample_id,
  country,
  app_version,
  client_id,
  newtab_visit_id,
  newtab_window_inner_width,
  SAFE_CAST(
    mozfun.map.get_key_with_null(event_details, 'section_position') AS INT
  ) AS section_position,
  SAFE_CAST(
    mozfun.map.get_key(event_details, 'is_section_followed') AS BOOLEAN
  ) AS is_section_followed,
  ANY_VALUE(ping_info.experiments) AS experiments,
  SAFE_CAST(mozfun.map.get_key(event_details, 'position') AS INT) AS position,
  SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN) AS is_sponsored,
  mozfun.map.get_key(event_details, 'format') AS format,
    -- each interaction should happen at most 1 time so look for any event in the visit
  SAFE_CAST(LOGICAL_OR(event_name = 'impression') AS INT) AS impression_count,
  SAFE_CAST(LOGICAL_OR(event_name = 'click') AS INT) AS clicks_count,
  SAFE_CAST(LOGICAL_OR(event_name = 'dismiss') AS INT) AS dismiss_count,
  SAFE_CAST(
    LOGICAL_OR(
      event_name IN ('thumb_voting_interaction')
      AND SAFE_CAST(mozfun.map.get_key(event_details, 'thumbs_up') AS BOOLEAN)
    ) AS INT
  ) AS thumbs_up_count,
  SAFE_CAST(
    LOGICAL_OR(
      event_name IN ('thumb_voting_interaction')
      AND SAFE_CAST(mozfun.map.get_key(event_details, 'thumbs_down') AS BOOLEAN)
    ) AS INT
  ) AS thumbs_down_count,
FROM
  pocket_events_unnested
INNER JOIN
  visit_window_width
  USING (newtab_visit_id)
GROUP BY
  ALL
