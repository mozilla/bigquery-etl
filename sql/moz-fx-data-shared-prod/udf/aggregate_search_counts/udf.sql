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
      COALESCE(SUM(IF(source = "urlbar-handoff", count, 0)), 0) AS search_count_urlbar_handoff,
      COALESCE(SUM(IF(source = "urlbar-persisted", count, 0)), 0) AS search_count_urlbar_persisted,
      COALESCE(SUM(IF(source = "webextension", count, 0)), 0) AS search_count_webextension,
      COALESCE(SUM(IF(source = "alias", count, 0)), 0) AS search_count_alias,
      COALESCE(
        SUM(IF(source = "urlbar-searchmode", count, 0)),
        0
      ) AS search_count_urlbar_searchmode,
      COALESCE(
        SUM(
          IF(
            source IN (
              'searchbar',
              'urlbar',
              'urlbar-handoff',
              'urlbar-persisted',
              'abouthome',
              'newtab',
              'contextmenu',
              'system',
              'activitystream',
              'webextension',
              'alias',
              'urlbar-searchmode'
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
  mozfun.assert.equals(
    STRUCT(
      5 AS search_count_abouthome,
      1 AS search_count_contextmenu,
      0 AS search_count_newtab,
      0 AS search_count_searchbar,
      0 AS search_count_system,
      0 AS search_count_urlbar,
      3 AS search_count_urlbar_handoff,
      0 AS search_count_urlbar_persisted,
      0 AS search_count_webextension,
      0 AS search_count_alias,
      0 AS search_count_urlbar_searchmode,
      9 AS search_count_all,
      0 AS search_count_tagged_sap,
      0 AS search_count_tagged_follow_on,
      0 AS search_count_organic
    ),
    udf.aggregate_search_counts(
      [
        STRUCT('foo' AS engine, 'abouthome' AS source, 3 AS count),
        STRUCT('foo' AS engine, 'abouthome' AS source, 2 AS count),
        STRUCT('foo' AS engine, 'contextmenu' AS source, 1 AS count),
        STRUCT('foo' AS engine, 'contextmenu' AS source, NULL AS count),
        STRUCT('foo' AS engine, 'urlbar-handoff' AS source, 3 AS count)
      ]
    )
  ),
  mozfun.assert.equals(
    STRUCT(
      0 AS search_count_all,
      0 AS search_count_abouthome,
      0 AS search_count_contextmenu,
      0 AS search_count_newtab,
      0 AS search_count_searchbar,
      0 AS search_count_system,
      0 AS search_count_urlbar,
      0 AS search_count_urlbar_handoff,
      0 AS search_count_urlbar_persisted,
      0 AS search_count_webextension,
      0 AS search_count_alias,
      0 AS search_count_urlbar_searchmode,
      0 AS search_count_tagged_sap,
      0 AS search_count_tagged_follow_on,
      0 AS search_count_organic
    ),
    udf.aggregate_search_counts([])
  );
