SELECT
  field_0  AS category,
  field_1 AS name,
  field_2 AS asin,
  field_3 AS seller,
  field_4 AS tracking_id,
  SAFE_CAST(field_5 AS TIMESTAMP) AS date_shipped,
  SAFE_CAST(field_6 AS FLOAT64) AS price_{{ currency }},
  SAFE_CAST(field_7 AS INT64) AS items_shipped,
  SAFE_CAST(field_8 AS INT64) AS returns,
  SAFE_CAST(field_9 AS FLOAT64) AS revenue_{{ currency }},
  SAFE_CAST(field_10 AS FLOAT64) AS ad_fees_{{ currency }},
  field_11 AS device_type_group,
FROM
  {{ table_id }}
WHERE
  {date_filter}
