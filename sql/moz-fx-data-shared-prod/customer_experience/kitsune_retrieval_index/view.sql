CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.customer_experience.kitsune_retrieval_index`
AS
SELECT
  * EXCEPT (product),
  CASE
    product
    WHEN "firefox"
      THEN "Firefox Desktop"
    WHEN "mobile"
      THEN "Fenix"
    WHEN "ios"
      THEN "Firefox iOS"
    WHEN "firefox-enterprise"
      THEN "Firefox Enterprise"
    ELSE "Non Firefox"
  END AS product,
  EXP(-DATE_DIFF(CURRENT_DATE(), creation_date, DAY) / 30) AS recency_score
FROM
  `moz-fx-data-shared-prod.customer_experience_derived.kitsune_retrieval_index_v1`
WHERE
  metadata.embedding_succeeded
