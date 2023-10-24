/*
Stably hash a client_id to an integer between 0 and 99, or NULL if client_id isn't 36 bytes
*/
CREATE OR REPLACE FUNCTION udf.safe_sample_id(client_id STRING) AS (
  MOD(udf.safe_crc32_uuid(CAST(client_id AS BYTES)), 100)
);

-- Tests
SELECT
  mozfun.assert.equals(sample_id, udf.safe_sample_id(client_id))
FROM
  UNNEST(
    [
      STRUCT(15 AS sample_id, "b50f76bc-fce4-4345-8d7c-4983f0967488" AS client_id),
      (99, "738b7ccd-60b1-47f0-98eb-3d2314559283"),
      (0, "ed4ed818-63a3-4347-89d7-eae53df14392"),
      (NULL, "length != 36"),
      (NULL, CAST(NULL AS STRING))
    ]
  )
