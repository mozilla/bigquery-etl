CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.customer_experience.zendesk_retrieval_index`
AS
SELECT
  t.* EXCEPT (product),
  COALESCE(m.product_mapping, t.product) AS product,
  EXP(-DATE_DIFF(CURRENT_DATE(), creation_date, DAY) / 30) AS recency_score
FROM
  `moz-fx-data-shared-prod.customer_experience_derived.zendesk_retrieval_index_v1` t
LEFT JOIN
  `moz-fx-data-shared-prod.static.cx_product_mappings_v1` m
  ON m.product = t.product
  AND m.source = 'Zendesk'
WHERE
  metadata.embedding_succeeded
