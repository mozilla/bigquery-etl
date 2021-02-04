CREATE OR REPLACE PROCEDURE
  event_analysis.create_count_steps_query(
    project STRING,
    dataset STRING,
    events ARRAY<STRUCT<category STRING, event_name STRING>>,
    OUT sql STRING
  )
BEGIN
  DECLARE i INT64 DEFAULT 1;

  DECLARE event STRUCT<category STRING, event_name STRING>;

  DECLARE event_filter STRING;

  DECLARE event_filters ARRAY<STRING> DEFAULT[];

  WHILE
    i <= ARRAY_LENGTH(events)
  DO
    SET event = events[ORDINAL(i)];

    SET event_filter = CONCAT(
      '(category = "',
      event.category,
      '"',
      ' AND event = "',
      event.event_name,
      '")'
    );

    SET event_filters = ARRAY_CONCAT(event_filters, [event_filter]);

    SET i = i + 1;
  END WHILE;

  SET sql = CONCAT(
    '\n  SELECT',
    '\n    event_analysis.aggregate_match_strings(ARRAY_AGG(event_analysis.event_index_to_match_string(index))) AS count_regex',
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
END;

-- Tests
BEGIN
  DECLARE result_sql STRING;

  DECLARE expect STRING DEFAULT """
  SELECT
    event_analysis.aggregate_match_strings(ARRAY_AGG(event_analysis.event_index_to_match_string(index))) AS count_regex
  FROM
    `moz-fx-data-shared-prod`.org_mozilla_firefox.event_types
  WHERE
    (category = "collections" AND event = "saved") OR (category = "collections" AND event = "tabs_added")
""";

  CALL event_analysis.create_count_steps_query(
    'moz-fx-data-shared-prod',
    'org_mozilla_firefox',
    [
      STRUCT('collections' AS category, 'saved' AS event_name),
      STRUCT('collections' AS category, 'tabs_added' AS event_name)
    ],
    result_sql
  );

  SELECT
    assert.sql_equals(expect, result_sql);
END;
