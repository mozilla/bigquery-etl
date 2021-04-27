SELECT
  SAFE_CAST(field_0 AS DATE) AS date,
  SAFE_CAST(field_1 AS INT64) AS clicks,
  SAFE_CAST(field_2 AS INT64) AS items_ordered_amazon,
  SAFE_CAST(field_3 AS INT64) AS items_ordered_3rd_party,
  SAFE_CAST(field_4 AS INT64) AS total_items_ordered,
  SAFE_CAST(field_5 AS FLOAT64) AS conversion,
FROM
  {{ table_id }}
WHERE
  {date_filter}
