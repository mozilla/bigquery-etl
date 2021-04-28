SELECT
  field_0 AS name,
  field_1 AS asin,
  SAFE_CAST(field_2 AS INT64) AS clicks,
  SAFE_CAST(field_3 AS FLOAT64) AS conversion,
  SAFE_CAST(field_4 AS TIMESTAMP) AS date,
  SAFE_CAST(field_5 AS INT64) AS qty,
FROM
  {{ table_id }}
WHERE
  {date_filter}
