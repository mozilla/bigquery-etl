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
        a.key
    )
    SELECT
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(k, v))
    FROM
      summed
  )
);

WITH extracted AS (
  SELECT
    *
  FROM
    -- TODO: change this location
    `mozdata.tmp.mobile_event_flat`
),
labeled AS (
  SELECT
    client_id,
    ARRAY_AGG(
      IF(object = "bookmark" AND method = "open", (method, value), NULL) IGNORE NULLS
    ) AS bookmarks_open,
    ARRAY_AGG(
      IF(object = "bookmark" AND method = "add", (method, value), NULL) IGNORE NULLS
    ) AS bookmarks_add,
    ARRAY_AGG(
      IF(object = "bookmark" AND method = "delete", (method, value), NULL) IGNORE NULLS
    ) AS bookmarks_delete,
    -- TODO: bookmarks_view_list, have not observed view-list method yet
    ARRAY_AGG(
      IF(object = "reading-list" AND method = "add", (method, value), NULL) IGNORE NULLS
    ) AS reading_list_add,
    ARRAY_AGG(
      IF(object = "reading-list" AND method = "delete", (method, value), NULL) IGNORE NULLS
    ) AS reading_list_delete,
  FROM
    extracted
  GROUP BY
    1
),
aggregated AS (
  SELECT
    client_id,
  -- background can be calculated too
    SUM(
      IF(object = "app" AND method = "foreground", value, 0)
    ) AS counter_glean_validation_foreground_count,
    any_value(
      labeled_counter(bookmarks_open, ["awesomebar-results", "bookmarks-panel"])
    ) AS labeled_counter_bookmarks_open,
  -- NOTE: should share-menu actually be context-menu?
    any_value(
      labeled_counter(bookmarks_add, ["page-action-menu", "share-menu", "activity-stream"])
    ) AS labeled_counter_bookmarks_add,
    any_value(
      labeled_counter(bookmarks_delete, ["page-action-menu", "activity-stream", "bookmarks-panel"])
    ) AS labeled_counter_bookmarks_delete,
    SUM(IF(object = "reader-mode-open-button", value, 0)) AS counter_reader_mode_open,
    SUM(IF(object = "reader-mode-close-button", value, 0)) AS counter_reader_mode_close,
    any_value(
      labeled_counter(
        reading_list_add,
        ["reader-mode-toolbar", "share-extension", "page-action-menu"]
      )
    ) AS labeled_counter_reading_list_add,
    any_value(
      labeled_counter(reading_list_delete, ["reader-mode-toolbar", "reading-list-panel"])
    ) AS labeled_counter_reading_list_delete,
    SUM(
      IF(object = "reading-list-item" AND method = "open", value, 0)
    ) AS counter_reading_list_open,
    SUM(
      IF(object = "reading-list-item" AND method = "mark-as-read", value, 0)
    ) AS counter_reading_mark_read,
    SUM(
      IF(object = "reading-list-item" AND method = "mark-as-unread", value, 0)
    ) AS counter_reading_mark_unread,
    SUM(IF(object LIKE "qr-code%" AND method = "scan", value, 0)) AS counter_qr_code_scanned,
  FROM
    extracted
  JOIN
    labeled
  USING
    (client_id)
  GROUP BY
    1
)
SELECT
  STRUCT(client_id) AS client_info,
  STRUCT(
    STRUCT(
      counter_glean_validation_foreground_count AS glean_validation_foreground_count,
      counter_reader_mode_open AS reader_mode_open,
      counter_reader_mode_close AS reader_mode_close,
      counter_reading_list_open AS reading_list_open,
      counter_reading_list_read AS reading_list_read,
      counter_reading_list_unread AS reading_list_unread,
      counter_qr_code_scanned AS qr_code_scanned,
    ) AS counter,
    STRUCT(
      labeled_counter_bookmarks_open AS bookmarks_open,
      labeled_counter_bookmarks_add AS bookmarks_add,
      labeled_counter_bookmarks_delete AS bookmarks_delete,
      labeled_counter_reading_list_add AS reading_list_add,
      labeled_counter_reading_list_delete AS reading_list_delete
    ) AS labeled_counter
  ) AS metrics
FROM
  aggregated
