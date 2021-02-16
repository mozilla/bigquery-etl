CREATE TEMPORARY FUNCTION labeled_counter(
  `values` ARRAY<STRUCT<key STRING, value INT64>>,
  labels ARRAY<STRING>
) AS (
  (
    WITH summed AS (
      SELECT
        IF(a.key IN (select * from unnest(labels)), a.key, "__unknown__") AS k,
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
    -- bookmarks_view
  FROM
    extracted
  GROUP BY
    1
)
SELECT
  client_id,
  -- background can be calculated too
  SUM(IF(object = "app" AND method = "foreground", value, 0)) AS validation_foreground_count,
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
FROM
  extracted
JOIN
  labeled
USING
  (client_id)
GROUP BY
  1
