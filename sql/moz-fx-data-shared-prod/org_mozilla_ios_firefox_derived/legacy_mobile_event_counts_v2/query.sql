CREATE TEMPORARY FUNCTION labeled_counter(
  `values` ARRAY<STRUCT<key STRING, value INT64>>,
  labels ARRAY<STRING>
) AS (
  (
    WITH summed AS (
      SELECT
        IF(a.key IN (SELECT * FROM UNNEST(labels)), a.key, "__unknown__") AS k,
        SUM(a.value) AS v
      FROM
        UNNEST(`values`) AS a
      GROUP BY
        k
    )
    SELECT
      ARRAY_AGG(STRUCT<key STRING, value INT64>(k, v))
    FROM
      summed
  )
);

WITH extracted AS (
  SELECT
    *,
    DATE(submission_timestamp) AS submission_date,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.mobile_event_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_app_name = "Fennec"
    AND normalized_os = "iOS"
),
meta AS (
  SELECT
    * EXCEPT (events)
  FROM
    extracted
),
meta_ranked AS (
  SELECT
    t AS metadata,
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id,
        submission_date
      ORDER BY
        submission_timestamp DESC
    ) AS _n
  FROM
    meta t
),
meta_recent AS (
  SELECT
    metadata.*
  FROM
    meta_ranked
  WHERE
    _n = 1
),
unnested AS (
  SELECT
    * EXCEPT (events),
    `moz-fx-data-shared-prod`.udf.deanonymize_event(event).*
  FROM
    extracted,
    UNNEST(events) AS event
),
counts AS (
  SELECT
    client_id,
    submission_date,
    event_category AS category,
    event_method AS method,
    event_object AS object,
    event_string_value AS string_value,
    COUNT(*) AS value
  FROM
    unnested
  GROUP BY
    client_id,
    submission_date,
    category,
    method,
    object,
    string_value
),
aggregated AS (
  SELECT
    client_id,
    submission_date,
    -- background can be calculated too
    SUM(
      IF(object = "app" AND method = "foreground", value, 0)
    ) AS counter_glean_validation_foreground_count,
    labeled_counter(
      ARRAY_AGG(IF(object = "bookmark" AND method = "open", (method, value), NULL) IGNORE NULLS),
      ["awesomebar-results", "bookmarks-panel"]
    ) AS labeled_counter_bookmarks_open,
    -- NOTE: should share-menu actually be context-menu?
    labeled_counter(
      ARRAY_AGG(IF(object = "bookmark" AND method = "add", (method, value), NULL) IGNORE NULLS),
      ["page-action-menu", "share-menu", "activity-stream"]
    ) AS labeled_counter_bookmarks_add,
    labeled_counter(
      ARRAY_AGG(IF(object = "bookmark" AND method = "delete", (method, value), NULL) IGNORE NULLS),
      ["page-action-menu", "activity-stream", "bookmarks-panel"]
    ) AS labeled_counter_bookmarks_delete,
    labeled_counter(
      ARRAY_AGG(
        IF(
          object = "bookmarks-panel",
          CASE
            WHEN string_value = "app-menu"
              THEN ("app-menu", value)
            WHEN string_value LIKE "home-panel%"
              THEN ("home-panel", value)
            ELSE NULL
          END,
          NULL
        ) IGNORE NULLS
      ),
      ["home-panel", "app-menu"]
    ) AS labeled_counter_bookmarks_view_list,
    labeled_counter(
      ARRAY_AGG(IF(object = "tab" AND method = "add", (method, value), NULL) IGNORE NULLS),
      ["normal-tab", "private-tab"]
    ) AS labeled_counter_tabs_open,
    labeled_counter(
      ARRAY_AGG(IF(object = "tab" AND method = "close", (method, value), NULL) IGNORE NULLS),
      ["normal-tab", "private-tab"]
    ) AS labeled_counter_tabs_close,
    SUM(IF(object = "reader-mode-open-button", value, 0)) AS counter_reader_mode_open,
    SUM(IF(object = "reader-mode-close-button", value, 0)) AS counter_reader_mode_close,
    labeled_counter(
      ARRAY_AGG(IF(object = "reading-list" AND method = "add", (method, value), NULL) IGNORE NULLS),
      ["reader-mode-toolbar", "share-extension", "page-action-menu"]
    ) AS labeled_counter_reading_list_add,
    labeled_counter(
      ARRAY_AGG(
        IF(object = "reading-list" AND method = "delete", (method, value), NULL) IGNORE NULLS
      ),
      ["reader-mode-toolbar", "reading-list-panel"]
    ) AS labeled_counter_reading_list_delete,
    SUM(
      IF(object = "reading-list-item" AND method = "open", value, 0)
    ) AS counter_reading_list_open,
    SUM(
      IF(object = "reading-list-item" AND method = "mark-as-read", value, 0)
    ) AS counter_reading_list_mark_read,
    SUM(
      IF(object = "reading-list-item" AND method = "mark-as-unread", value, 0)
    ) AS counter_reading_list_mark_unread,
    SUM(IF(object LIKE "qr-code%" AND method = "scan", value, 0)) AS counter_qr_code_scanned,
  FROM
    counts
  GROUP BY
    client_id,
    submission_date
)
SELECT
  submission_timestamp,
  document_id,
  (SELECT AS STRUCT metadata.* EXCEPT (uri)) AS metadata,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  STRUCT(
    client_id,
    CAST(NULL AS string) AS android_sdk_version,
    metadata.uri.app_build_id AS app_build,
    metadata.uri.app_update_channel AS app_channel,
    metadata.uri.app_version AS app_display_version,
    arch AS architecture,
    "Apple" AS device_manufacturer,
    device AS device_model,
    locale,
    os,
    osversion AS os_version,
    CAST(NULL AS string) AS telemetry_sdk_build
  ) AS client_info,
  STRUCT(
    STRUCT(
      counter_glean_validation_foreground_count AS glean_validation_foreground_count,
      counter_reader_mode_open AS reader_mode_open,
      counter_reader_mode_close AS reader_mode_close,
      counter_reading_list_open AS reading_list_open,
      counter_reading_list_mark_read AS reading_list_mark_read,
      counter_reading_list_mark_unread AS reading_list_mark_unread,
      counter_qr_code_scanned AS qr_code_scanned
    ) AS counter,
    STRUCT(
      labeled_counter_bookmarks_view_list AS bookmarks_view_list,
      labeled_counter_bookmarks_open AS bookmarks_open,
      labeled_counter_bookmarks_add AS bookmarks_add,
      labeled_counter_bookmarks_delete AS bookmarks_delete,
      labeled_counter_tabs_open AS tabs_open,
      labeled_counter_tabs_close AS tabs_close,
      labeled_counter_reading_list_add AS reading_list_add,
      labeled_counter_reading_list_delete AS reading_list_delete
    ) AS labeled_counter
  ) AS metrics
FROM
  aggregated
JOIN
  meta_recent
  USING (client_id, submission_date)
