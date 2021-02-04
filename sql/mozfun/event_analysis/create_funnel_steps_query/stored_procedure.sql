CREATE OR REPLACE PROCEDURE
  event_analysis.create_funnel_steps_query(
    project STRING,
    dataset STRING,
    funnel ARRAY<STRUCT<list ARRAY<STRUCT<category STRING, event_name STRING>>>>,
    OUT sql STRING
  )
BEGIN
  DECLARE i INT64 DEFAULT 1;

  DECLARE event_filter STRING;

  DECLARE event STRUCT<category STRING, event_name STRING>;

  DECLARE event_filters ARRAY<STRING>;

  DECLARE funnel_step_i INT64;

  DECLARE funnel_step_events ARRAY<STRUCT<category STRING, event_name STRING>>;

  DECLARE funnel_step_filters STRING;

  DECLARE funnel_regex_assembly STRING;

  DECLARE funnel_event_filters ARRAY<STRING> DEFAULT[];

  DECLARE funnel_regex_assemblies ARRAY<STRING> DEFAULT[];

  WHILE
    i <= ARRAY_LENGTH(funnel)
  DO
    SET event_filters = [];

    SET funnel_step_i = 1;

    SET funnel_step_events = funnel[ORDINAL(i)].list;

    WHILE
      funnel_step_i <= ARRAY_LENGTH(funnel_step_events)
    DO
      SET event = funnel_step_events[ORDINAL(funnel_step_i)];

      SET event_filter = CONCAT(
        '(category = "',
        event.category,
        '"',
        ' AND event = "',
        event.event_name,
        '")'
      );

      SET event_filters = ARRAY_CONCAT(event_filters, [event_filter]);

      SET funnel_step_i = funnel_step_i + 1;
    END WHILE;

    SET funnel_step_filters = CONCAT(
      '\n  SELECT',
      '\n    ',
      i,
      ' AS step,',
      '\n    event_analysis.aggregate_match_strings(ARRAY_AGG(event_analysis.event_index_to_match_string(index))) AS step_regex',
      '\n  FROM',
      '\n    `',
      project,
      '`.',
      dataset,
      '.event_types',
      '\n  WHERE',
      '\n    ',
      ARRAY_TO_STRING(event_filters, ' OR ')
    );

    SET funnel_event_filters = ARRAY_CONCAT(funnel_event_filters, [funnel_step_filters]);

    SET funnel_regex_assembly = CONCAT(
      'event_analysis.create_funnel_regex(ARRAY_AGG(step_regex ORDER BY step LIMIT ',
      i,
      '), TRUE)'
    );

    SET funnel_regex_assemblies = ARRAY_CONCAT(funnel_regex_assemblies, [funnel_regex_assembly]);

    SET i = i + 1;
  END WHILE;

  SET sql = CONCAT(
    'WITH step_regexes AS (',
    ARRAY_TO_STRING(funnel_event_filters, '\n  UNION ALL\n'),
    '\n)',
    '\nSELECT',
    '\n  [',
    '\n    ',
    ARRAY_TO_STRING(funnel_regex_assemblies, ',\n'),
    '\n  ]',
    '\n  FROM',
    '\n    step_regexes'
  );
END;

-- Tests
BEGIN
  DECLARE result_sql STRING;

  DECLARE expect STRING DEFAULT """
WITH step_regexes AS (
  SELECT
    1 AS step,
    event_analysis.aggregate_match_strings(ARRAY_AGG(event_analysis.event_index_to_match_string(index))) AS step_regex
  FROM
    `moz-fx-data-shared-prod`.org_mozilla_firefox.event_types
  WHERE
    (category = "collections" AND event = "tab_select_opened")

  UNION ALL

  SELECT
    2 AS step,
    event_analysis.aggregate_match_strings(ARRAY_AGG(event_analysis.event_index_to_match_string(index))) AS step_regex
  FROM
    `moz-fx-data-shared-prod`.org_mozilla_firefox.event_types
  WHERE
    (category = "collections" AND event = "saved") OR (category = "collections" AND event = "tabs_added")
)

SELECT
  [
    event_analysis.create_funnel_regex(ARRAY_AGG(step_regex ORDER BY step LIMIT 1), TRUE),
    event_analysis.create_funnel_regex(ARRAY_AGG(step_regex ORDER BY step LIMIT 2), TRUE)
  ]
FROM
  step_regexes
""";

  CALL event_analysis.create_funnel_steps_query(
    'moz-fx-data-shared-prod',
    'org_mozilla_firefox',
    [
      STRUCT([STRUCT('collections' AS category, 'tab_select_opened' AS event_name)] AS list),
      STRUCT(
        [
          STRUCT('collections' AS category, 'saved' AS event_name),
          STRUCT('collections' AS category, 'tabs_added' AS event_name)
        ] AS list
      )
    ],
    result_sql
  );

  SELECT
    assert.sql_equals(expect, result_sql);
END;
