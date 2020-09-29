CREATE OR REPLACE PROCEDURE
  event_analysis.get_count_sql(
    project STRING,
    dataset STRING,
    count_name STRING,
    events ARRAY<STRUCT<category STRING, event_name STRING>>,
    OUT count_sql STRING
  )
BEGIN
  DECLARE i INT64 DEFAULT 1;

  DECLARE regex_query STRING;

  DECLARE count_regex STRING;

  CALL event_analysis.create_count_steps_query(project, dataset, events, regex_query);

  EXECUTE IMMEDIATE regex_query INTO count_regex;

  SET count_sql = CONCAT(
    'ARRAY_LENGTH(regexp_extract_all(events, r"',
    count_regex,
    '")) AS ',
    count_name
  );
END;

-- See create_funnel_steps_query/stored_procedure.sql for tests
