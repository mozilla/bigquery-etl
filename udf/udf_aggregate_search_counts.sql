CREATE TEMP FUNCTION
  udf_aggregate_search_counts(search_counts ARRAY<STRUCT<list ARRAY<STRUCT<element STRUCT<engine STRING,
    source STRING,
    count INT64>>>>>) AS ((
    SELECT
      AS STRUCT --
      SUM(element.count) AS search_count_all,
      SUM(IF(element.source = "abouthome",
          element.count,
          0)) AS search_count_abouthome,
      SUM(IF(element.source = "contextmenu",
          element.count,
          0)) AS search_count_contextmenu,
      SUM(IF(element.source = "newtab",
          element.count,
          0)) AS search_count_newtab,
      SUM(IF(element.source = "searchbar",
          element.count,
          0)) AS search_count_searchbar,
      SUM(IF(element.source = "system",
          element.count,
          0)) AS search_count_system,
      SUM(IF(element.source = "urlbar",
          element.count,
          0)) AS search_count_urlbar
    FROM
      UNNEST(search_counts),
      UNNEST(list)
    WHERE
      element.source IN ("abouthome",
        "contextmenu",
        "newtab",
        "searchbar",
        "system",
        "urlbar")));
