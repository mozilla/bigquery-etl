/*

Accepts a pipeline metadata struct as input and returns a modified struct that
includes a few parsed or normalized variants of the input metadata fields.

*/

CREATE TEMP FUNCTION
  udf_normalize_main_payload(payload ANY TYPE) AS ((
    SELECT
      AS STRUCT payload.* REPLACE ( (
        SELECT
          AS STRUCT payload.info.* REPLACE (
          IF
            (payload.info.session_length < 0,
              NULL,
              payload.info.session_length) AS session_length)) AS info)) );

-- Tests

SELECT
assert_equals(
  3,
  udf_normalize_main_payload(
    STRUCT(
      STRUCT(
        3 AS session_length
      ) AS info)
  ).info.session_length),
assert_null(
  udf_normalize_main_payload(
    STRUCT(
      STRUCT(
        -98984108 AS session_length
      ) AS info)
    ).info.session_length);
