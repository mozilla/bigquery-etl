/*
Create a view that contains funnels and event counts.

Funnels are comprised of steps, and each step has a boolean that
indicates whether that user completed that funnel step.

@param project - The project the source table (`event_types`) lives in
@param dataset - The dataset the source table lives in
@param funnel - An array of funnel steps. Each funnel step is one or more events
                that, if fired, indicate the user completed that step of the funnel.
@param funnel_regex - Output. Array of regex strings, each corresponding to completing up to that funnel step.
*/
CREATE OR REPLACE PROCEDURE
  create_funnel_steps(view_name STRING, project STRING, dataset STRING, funnels ARRAY<STRUCT<funnel_name STRING, funnel ARRAY<STRUCT<step_name STRING, events ARRAY<STRUCT<category STRING, event_name STRING>>>>>>, counts ARRAY<STRUCT<count_name STRING, events ARRAY<STRUCT<category STRING, event_name STRING>>>>)
BEGIN

  -- Output view name
  DECLARE full_view_name STRING DEFAULT CONCAT("`moz-fx-data-shared-prod`.analysis.", table_name);

  -- Outer-loop index
  DECLARE i INT64 DEFAULT 0;

  -- Funnel vars
  DECLARE current_funnel_sql STRING;
  DECLARE funnel_regex ARRAY<STRING>;
  DECLARE funnel_sqls ARRAY<STRING> DEFAULT [];
  DECLARE events ARRAY<STRUCT<list ARRAY<STRUCT<category STRING, event_name STRING>>>>;

  -- Count vars
  DECLARE current_count_sql STRING;
  DECLARE count_regex STRING;
  DECLARE count_sqls ARRAY<STRING> DEFAULT [];

  WHILE i < ARRAY_LENGTH(funnels) DO
    -- Retrieve the funnel, and get the associated regex for each step
    SET events = ARRAY(SELECT STRUCT(events AS list) FROM UNNEST(funnels[OFFSET(i)].funnel));
    CALL procedures.create_funnel_steps(project, dataset, events, funnel_regex);

    -- Create a single SQL struct that represents each step of that funnel
    SET current_funnel_sql = CONCAT(
      "STRUCT(",
        -- Each funnel has a struct of steps
        ARRAY_TO_STRING(
          (
            SELECT
              ARRAY_AGG( -- Each step has a boolean (did they complete it?) and a name
                CONCAT("REGEXP_CONTAINS(events, '", regex, "') AS ",
                funnels[OFFSET(i)].funnel[OFFSET(n)].step_name)
              )
            FROM
              UNNEST(funnel_regex) AS regex
              WITH OFFSET AS n  
          ),
          ", "
        ),
      ") AS ",
      funnels[OFFSET(i)].funnel_name);

    SET funnel_sqls = ARRAY_CONCAT(funnel_sqls, [current_funnel_sql]);
    SET i = i + 1;
  END WHILE;

  SET i = 0;
  WHILE i < ARRAY_LENGTH(counts) DO
    CALL procedures.create_count_regex(project, dataset, counts[OFFSET(i)].events, count_regex);

    SET current_count_sql = CONCAT(
      "ARRAY_LENGTH(regexp_extract_all(events, '", count_regex, "')) ",
      "AS ", counts[OFFSET(i)].count_name);

    SET count_sqls = ARRAY_CONCAT(count_sqls, [current_count_sql]);
    SET i = i + 1;
  END WHILE;

  EXECUTE IMMEDIATE CONCAT(
    "CREATE OR REPLACE VIEW ",
    view_name,
    " AS SELECT ",
    ARRAY_TO_STRING(ARRAY_CONCAT(funnel_sqls, count_sqls), ","),
    ", * FROM `", project, "`.", dataset, ".events_daily"
  );
END
