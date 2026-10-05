SELECT
  creation_date
FROM
  `moz-fx-data-shared-prod.customer_experience_derived.kitsune_retrieval_index_v1`
WHERE
  DATE(creation_date) = @submission_date
LIMIT
  1
