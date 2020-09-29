CREATE OR REPLACE PROCEDURE
  event_analysis.create_events_view(
    view_name STRING,
    project STRING,
    dataset STRING,
    funnels ARRAY<
      STRUCT<
        funnel_name STRING,
        funnel ARRAY<
          STRUCT<step_name STRING, events ARRAY<STRUCT<category STRING, event_name STRING>>>
        >
      >
    >,
    counts ARRAY<
      STRUCT<count_name STRING, events ARRAY<STRUCT<category STRING, event_name STRING>>>
    >
  )
BEGIN
  DECLARE i INT64 DEFAULT 1;

  DECLARE funnel_sql STRING;

  DECLARE funnel_sqls ARRAY<STRING> DEFAULT[];

  DECLARE count_sql STRING;

  DECLARE count_sqls ARRAY<STRING> DEFAULT[];

  WHILE
    i <= ARRAY_LENGTH(funnels)
  DO
    CALL event_analysis.get_funnel_steps_sql(
      project,
      dataset,
      funnels[ORDINAL(i)].funnel_name,
      funnels[ORDINAL(i)].funnel,
      funnel_sql
    );

    SET funnel_sqls = ARRAY_CONCAT(funnel_sqls, [funnel_sql]);

    SET i = i + 1;
  END WHILE;

  SET i = 1;

  WHILE
    i <= ARRAY_LENGTH(counts)
  DO
    CALL event_analysis.get_count_sql(
      project,
      dataset,
      counts[ORDINAL(i)].count_name,
      counts[ORDINAL(i)].events,
      count_sql
    );

    SET count_sqls = ARRAY_CONCAT(count_sqls, [count_sql]);

    SET i = i + 1;
  END WHILE;

  EXECUTE IMMEDIATE CONCAT(
    '\nCREATE OR REPLACE VIEW ',
    '\n  `',
    project,
    '`.analysis.',
    view_name,
    '\nAS',
    '\nSELECT ',
    '\n  ',
    ARRAY_TO_STRING(ARRAY_CONCAT(funnel_sqls, count_sqls), ',\n'),
    ',',
    '\n  *',
    '\nFROM',
    '\n  `',
    project,
    '`.',
    dataset,
    '.events_daily'
  );
END;

-- See create_funnel_steps_query/stored_procedure.sql for tests
