-- {'name': 'kitsune_retrieval_index_unclassified_product', 'alert_type': 'count', 'range': {'max': 0}, 'collections': ['Operational Checks'], 'schedule': 'Default Schedule - 13:00 UTC'}
SELECT
  COUNT(*) AS unclassified_product_count
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND product = 'unclassified';
