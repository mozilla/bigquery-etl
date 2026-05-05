/*

Accepts a pipeline metadata struct as input and returns a modified struct that
includes a few parsed or normalized variants of the input metadata fields.

*/
CREATE OR REPLACE FUNCTION udf.normalize_main_payload(payload ANY TYPE) AS (
  (
    SELECT AS STRUCT
      payload.* REPLACE (
        (
          SELECT AS STRUCT
            payload.info.* REPLACE (
              IF(
                -- Bug 1592012 - some clients report bogus negative session lengths;
                -- we also filter out values longer than 1 year, which is highly unlikely.
                payload.info.session_length
                BETWEEN 0
                AND 31536000,
                payload.info.session_length,
                NULL
              ) AS session_length
            )
        ) AS info
      )
  )
);

-- Tests
SELECT
  mozfun.assert.equals(
    3,
    udf.normalize_main_payload(STRUCT(STRUCT(3 AS session_length) AS info)).info.session_length
  ),
  mozfun.assert.null(
    udf.normalize_main_payload(
      STRUCT(STRUCT(-98984108 AS session_length) AS info)
    ).info.session_length
  ),
  mozfun.assert.null(
    udf.normalize_main_payload(
      STRUCT(STRUCT(51536000 AS session_length) AS info)
    ).info.session_length
  );
