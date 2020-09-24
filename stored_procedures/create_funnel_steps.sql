/*
Retrieve a list of regex expressions that correspond to completing each step of a funnel.

@param project - The project the source table (`event_types`) lives in
@param dataset - The dataset the source table lives in
@param funnel - An array of funnel steps. Each funnel step is one or more events
                that, if fired, indicate the user completed that step of the funnel.
@param funnel_regex - Output. Array of regex strings, each corresponding to completing up to that funnel step.
*/
CREATE OR REPLACE PROCEDURE
  create_funnel_steps(project STRING, dataset STRING, funnel ARRAY<STRUCT<list ARRAY<STRUCT<category STRING, event_name STRING>>>>, OUT funnel_regex ARRAY<STRING>)
BEGIN

  -- Outer loop index
  DECLARE i INT64 DEFAULT 0;

  -- Per-event variables
  DECLARE event_query STRING;
  DECLARE event STRUCT<category STRING, event_name STRING>;

  -- Per-funnel step variables
  DECLARE funnel_step_events ARRAY<STRUCT<category STRING, event_name STRING>>;
  DECLARE funnel_step_i INT64 DEFAULT 0;
  DECLARE funnel_step_regex STRING;
  DECLARE funnel_step_regexes ARRAY<STRING> DEFAULT [];

  -- Per-funnel variables
  DECLARE step_regex STRING;
  DECLARE step_regexes ARRAY<STRING> DEFAULT [];

  SET funnel_regex = [];
  WHILE i < array_length(funnel) DO
    SET funnel_step_events = funnel[OFFSET(i)].list;

    WHILE funnel_step_i < array_length(funnel_step_events) DO
      SET event = funnel_step_events[OFFSET(funnel_step_i)];

      SET event_query = CONCAT(
        "SELECT udf.event_index_to_match_string(index) ",
        "FROM `", project, "`.", dataset, ".event_types ",
        "WHERE category = '", event.category, "'",
        "      AND event = '", event.event_name, "'"
      );

      EXECUTE IMMEDIATE event_query INTO funnel_step_regex;

      -- update per-funnel-step vars
      SET funnel_step_regexes = ARRAY_CONCAT(funnel_step_regexes, [funnel_step_regex]);
      SET funnel_step_i = funnel_step_i + 1;
    END WHILE;

    -- update per-funnel vars
    SET i = i+1;
    SET step_regexes = ARRAY_CONCAT(step_regexes, [udf.aggregate_match_strings(funnel_step_regexes)]);
    SET funnel_regex = ARRAY_CONCAT(funnel_regex, [udf.create_funnel_regex(step_regexes, TRUE)]);

    -- reset per-funnel-step vars
    SET funnel_step_i = 0;
    SET funnel_step_regexes = [];
  END WHILE;
END
