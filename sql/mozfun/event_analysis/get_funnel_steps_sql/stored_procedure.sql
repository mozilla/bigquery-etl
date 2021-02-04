CREATE OR REPLACE PROCEDURE
  event_analysis.get_funnel_steps_sql(
    project STRING,
    dataset STRING,
    funnel_name STRING,
    funnel ARRAY<STRUCT<step_name STRING, list ARRAY<STRUCT<category STRING, event_name STRING>>>>,
    OUT funnel_sql STRING
  )
BEGIN
  DECLARE i INT64 DEFAULT 1;

  DECLARE regex_query STRING;

  DECLARE funnel_sqls ARRAY<STRING> DEFAULT[];

  DECLARE funnel_step_regex ARRAY<STRING>;

  DECLARE step_sql STRING;

  CALL event_analysis.create_funnel_steps_query(
    project,
    dataset,
    ARRAY(SELECT AS STRUCT list FROM UNNEST(funnel)),
    regex_query
  );

  EXECUTE IMMEDIATE regex_query INTO funnel_step_regex;

  WHILE
    i <= ARRAY_LENGTH(funnel)
  DO
    SET step_sql = CONCAT(
      'REGEXP_CONTAINS(events, r"',
      funnel_step_regex[ORDINAL(i)],
      '") AS ',
      funnel[ORDINAL(i)].step_name
    );

    SET funnel_sqls = ARRAY_CONCAT(funnel_sqls, [step_sql]);

    SET i = i + 1;
  END WHILE;

  SET funnel_sql = CONCAT('STRUCT(', ARRAY_TO_STRING(funnel_sqls, ', '), ') AS ', funnel_name);
END;

-- See create_funnel_steps_query/stored_procedure.sql for tests
