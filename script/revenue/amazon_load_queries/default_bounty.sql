SELECT
  field_0 AS name,
  SAFE_CAST(field_1 AS DATE) AS date_shipped,
  field_2 AS tracking_id,
  SAFE_CAST(field_3 AS INT64) AS quantity,
  SAFE_CAST(field_4 AS FLOAT64) AS ad_fees_{{ currency }},
FROM
  {{ table_id }}
WHERE
  {date_filter}
