CREATE TEMP FUNCTION udf_aggregate_search_counts(
  search_counts ARRAY<
    STRUCT<
      engine STRING,
      source STRING,
      count INT64
    >
  >
) AS (
  (
    SELECT AS STRUCT
      SUM(count) AS search_count_all,
      SUM(IF(
        source = "abouthome",
        count,
        0
      )) AS search_count_abouthome,
      SUM(IF(
        source = "contextmenu",
        count,
        0
      )) AS search_count_contextmenu,
      SUM(IF(
        source = "newtab",
        count,
        0
      )) AS search_count_newtab,
      SUM(IF(
        source = "searchbar",
        count,
        0
      )) AS search_count_searchbar,
      SUM(IF(
        source = "system",
        count,
        0
      )) AS search_count_system,
      SUM(IF(
        source = "urlbar",
        count,
        0
      )) AS search_count_urlbar
    FROM
      UNNEST(search_counts)
    WHERE
      source IN (
        "abouthome",
        "contextmenu",
        "newtab",
        "searchbar",
        "system",
        "urlbar"
      )
  )
);
