(
    SELECT
      ARRAY_AGG(
        STRUCT<key STRING, value FLOAT64>(
          CAST(ROUND((key/10),1) AS STRING),
          `values`[OFFSET(SAFE_CAST(key AS INT64))]
        )
        ORDER BY
          key
      )
    FROM
      UNNEST(required) AS key
  )