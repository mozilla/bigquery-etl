CREATE OR REPLACE FUNCTION udf.aggregate_search_counts(
  search_counts ARRAY<STRUCT<engine STRING, source STRING, count INT64>>
) AS (
  (
    SELECT AS STRUCT
      COALESCE(SUM(IF(source = "abouthome", count, 0)), 0) AS search_count_abouthome,
      COALESCE(SUM(IF(source = "contextmenu", count, 0)), 0) AS search_count_contextmenu,
      COALESCE(SUM(IF(source = "newtab", count, 0)), 0) AS search_count_newtab,
      COALESCE(SUM(IF(source = "searchbar", count, 0)), 0) AS search_count_searchbar,
      COALESCE(SUM(IF(source = "system", count, 0)), 0) AS search_count_system,
      COALESCE(SUM(IF(source = "urlbar", count, 0)), 0) AS search_count_urlbar,
      COALESCE(
        SUM(
          IF(
            source IN (
              'searchbar',
              'urlbar',
              'abouthome',
              'newtab',
              'contextmenu',
              'system',
              'activitystream',
              'webextension',
              'alias'
            )
            OR source IS NULL,
            count,
            0
          )
        ),
        0
      ) AS search_count_all,
      COALESCE(
        SUM(IF(STARTS_WITH(source, 'in-content:sap:') OR STARTS_WITH(source, 'sap:'), count, 0)),
        0
      ) AS search_count_tagged_sap,
      COALESCE(
        SUM(
          IF(
            STARTS_WITH(source, 'in-content:sap-follow-on:')
            OR STARTS_WITH(source, 'follow-on:'),
            count,
            0
          )
        ),
        0
      ) AS search_count_tagged_follow_on,
      COALESCE(
        SUM(IF(STARTS_WITH(source, 'in-content:organic:'), count, 0)),
        0
      ) AS search_count_organic
    FROM
      UNNEST(search_counts)
  )
);

-- Tests
SELECT
  assert_equals(
    STRUCT(
      5 AS search_count_abouthome,
      1 AS search_count_contextmenu,
      0 AS search_count_newtab,
      0 AS search_count_searchbar,
      0 AS search_count_system,
      0 AS search_count_urlbar,
      6 AS search_count_all,
      0 AS search_count_tagged_sap,
      0 AS search_count_tagged_follow_on,
      0 AS search_count_organic
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
      0 AS search_count_urlbar,
      0 AS search_count_tagged_sap,
      0 AS search_count_tagged_follow_on,
      0 AS search_count_organic
    ),
    udf.aggregate_search_counts([])
  );
