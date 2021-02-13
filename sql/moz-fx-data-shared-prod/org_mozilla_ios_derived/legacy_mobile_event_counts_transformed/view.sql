CREATE TEMPORARY FUNCTION labeled_counter(`values` ARRAY<STRUCT<key STRING, value INT64>>) AS (
  (
    WITH summed AS (
      SELECT
        a.key AS k,
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
    ARRAY_AGG(IF(object = "bookmark", (method, value), NULL) IGNORE NULLS) AS bookmark
  FROM
    extracted
  GROUP BY
    1
)
SELECT
  client_id,
  -- background is can be calculated too
  SUM(IF(object = "app" AND method = "foreground", value, 0)) AS validation_foreground_count,
  any_value(labeled_counter(bookmark)) AS labeled_counter_bookmarks,
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
