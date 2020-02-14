CREATE OR REPLACE FUNCTION udf.aggregate_search_counts(
  search_counts ARRAY<STRUCT<engine STRING, source STRING, count INT64>>
) AS (
  (
    SELECT AS STRUCT
      COALESCE(SUM(count), 0) AS search_count_all,
      COALESCE(SUM(IF(source = "abouthome", count, 0)), 0) AS search_count_abouthome,
      COALESCE(SUM(IF(source = "contextmenu", count, 0)), 0) AS search_count_contextmenu,
      COALESCE(SUM(IF(source = "newtab", count, 0)), 0) AS search_count_newtab,
      COALESCE(SUM(IF(source = "searchbar", count, 0)), 0) AS search_count_searchbar,
      COALESCE(SUM(IF(source = "system", count, 0)), 0) AS search_count_system,
      COALESCE(SUM(IF(source = "urlbar", count, 0)), 0) AS search_count_urlbar
    FROM
      UNNEST(search_counts)
    WHERE
      source IN ("abouthome", "contextmenu", "newtab", "searchbar", "system", "urlbar")
  )
);

SELECT
  assert_equals(
    STRUCT(
      6 AS search_count_all,
      5 AS search_count_abouthome,
      1 AS search_count_contextmenu,
      0 AS search_count_newtab,
      0 AS search_count_searchbar,
      0 AS search_count_system,
      0 AS search_count_urlbar
    ),
    udf.aggregate_search_counts(
      [
        STRUCT('foo' AS engine, 'abouthome' AS source, 3 AS count),
        STRUCT('foo' AS engine, 'abouthome' AS source, 2 AS count),
        STRUCT('foo' AS engine, 'contextmenu' AS source, 1 AS count),
        STRUCT('foo' AS engine, 'contextmenu' AS source, NULL AS count)
      ]
    )
  ),
  assert_equals(
    STRUCT(
      0 AS search_count_all,
      0 AS search_count_abouthome,
      0 AS search_count_contextmenu,
      0 AS search_count_newtab,
      0 AS search_count_searchbar,
      0 AS search_count_system,
      0 AS search_count_urlbar
    ),
    udf.aggregate_search_counts([])
  );
