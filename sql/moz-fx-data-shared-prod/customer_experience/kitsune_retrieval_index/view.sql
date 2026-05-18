CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.customer_experience.kitsune_retrieval_index`
AS
SELECT
  t.* EXCEPT (product),
  COALESCE(m.product_mapping, t.product) AS product,
  (
    STARTS_WITH(COALESCE(m.product_mapping, t.product), 'Firefox')
    OR COALESCE(m.product_mapping, t.product) IN (
      'Fenix',
      'Klar iOS',
      'Klar Android',
      'Focus iOS',
      'Focus Android'
    )
  ) AS is_firefox_product,
  EXP(-DATE_DIFF(CURRENT_DATE(), creation_date, DAY) / 30) AS recency_score
FROM
  `moz-fx-data-shared-prod.customer_experience_derived.kitsune_retrieval_index_v1` t
LEFT JOIN
  (
    SELECT
      product,
      MAX(product_mapping) AS product_mapping
    FROM
      `moz-fx-data-shared-prod.static.cx_product_mappings_v1`
    WHERE
      source = 'Kitsune'
    GROUP BY
      product
  ) m
  ON m.product = t.product
WHERE
  metadata.embedding_succeeded
