CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.customer_experience.zendesk_retrieval_index`
AS
SELECT
  * EXCEPT (product),
  mozfun.customer_experience.cx_normalize_product(product, 'Zendesk') AS product,
  EXP(-DATE_DIFF(CURRENT_DATE(), creation_date, DAY) / 30) AS recency_score
FROM
  `moz-fx-data-shared-prod.customer_experience_derived.zendesk_retrieval_index_v1`
WHERE
  metadata.embedding_succeeded
