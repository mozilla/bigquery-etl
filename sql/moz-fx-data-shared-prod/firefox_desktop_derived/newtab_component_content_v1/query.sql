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
    DATE(submission_timestamp) = '2025-06-01'
  GROUP BY
    newtab_visit_id
),
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
    DATE(submission_timestamp) = '2025-06-01'
    AND category = 'pocket'
),
raw_content_info AS (
  SELECT
    submission_date,
    sample_id,
    country,
    app_version,
    client_id,
    newtab_visit_id,
    SAFE_CAST(
      mozfun.map.get_key_with_null(event_details, 'section_position') AS INT
    ) AS section_position,
    CAST(
      mozfun.map.get_key(event_details, 'is_section_followed') AS BOOLEAN
    ) AS is_section_followed,
    ANY_VALUE(ping_info.experiments) AS experiments,
    CAST(mozfun.map.get_key(event_details, 'position') AS INT) AS position,
    CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN) AS is_sponsored,
    mozfun.map.get_key(event_details, 'format') AS format,
    -- each interaction should happen at most 1 time so look for any event in the visit
    CAST(LOGICAL_OR(event_name = 'impression') AS INT) AS impression_count,
    CAST(LOGICAL_OR(event_name = 'click') AS INT) AS clicks_count,
    CAST(LOGICAL_OR(event_name = 'dismiss') AS INT) AS dismiss_count,
    CAST(
      LOGICAL_OR(
        event_name IN ('thumb_voting_interaction')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'thumbs_up') AS BOOLEAN)
      ) AS INT
    ) AS thumbs_up_count,
    CAST(
      LOGICAL_OR(
        event_name IN ('thumb_voting_interaction')
        AND SAFE_CAST(mozfun.map.get_key(event_details, 'thumbs_down') AS BOOLEAN)
      ) AS INT
    ) AS thumbs_down_count,
  FROM
    pocket_events_unnested
  GROUP BY
    ALL
),
-- the purpose of the final CTEs is to apply the necessary UDFs to get rownumber
content_and_visit_info AS (
  SELECT
    raw_content_info.*,
    mozfun.newtab.determine_grid_layout_v1(
      section_position IS NOT NULL,
      app_version,
      experiments
    ) AS layout_type,
    newtab_window_inner_width
  FROM
    raw_content_info
  INNER JOIN
    visit_window_width
    USING (newtab_visit_id)
),
add_tiles_per_row AS (
  SELECT
    content_and_visit_info.*,
    mozfun.newtab.determine_tiles_per_row_v1(
      layout_type,
      newtab_window_inner_width
    ) AS num_tiles_per_row
  FROM
    content_and_visit_info
)
SELECT
  add_tiles_per_row.*,
  CAST(
    CASE
      WHEN layout_type = 'SECTION_GRID'
        -- the row number is the same as the section position in sections
        THEN section_position
        -- for grid, divide the postition by the number of tiles per row and take the floor
      ELSE FLOOR(position / num_tiles_per_row)
    END AS INT
  ) AS row_number
FROM
  add_tiles_per_row
