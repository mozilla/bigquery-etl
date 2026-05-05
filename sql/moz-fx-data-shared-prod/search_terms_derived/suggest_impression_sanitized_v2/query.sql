/*

This query simply copies data from a table in the restricted suggest-searches-prod
project to shared-prod, but throws an error on empty input in order to signal an
upstream delay or error.

In order to produce the error, it includes aggregations and join steps.

*/
WITH sanitized_impressions AS (
  SELECT
    *
  FROM
    `suggest-searches-prod-a30f.sanitized.suggest_impression_sanitized_v2`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
sanitized_impressions_count AS (
  SELECT
    COUNT(*) AS _n,
    COUNT(sanitized_query) AS _n_with_query,
  FROM
    sanitized_impressions
),
-- We perform a LEFT JOIN on TRUE as a workaround to attach the count to every
-- row from the impressions table; the LEFT JOIN has the important property that
-- if the input impressions partition is empty, we will still get a single row of
-- output, which allows us to raise an error in the WHERE clause.
validated_impressions AS (
  SELECT
    * EXCEPT (_n, _n_with_query),
  FROM
    sanitized_impressions_count
  LEFT JOIN
    sanitized_impressions
    ON TRUE
  WHERE
    IF(
      _n < 1,
      ERROR(
        "The source partition of suggest-searches-prod-a30f.sanitized.suggest_impression_sanitized_v2 is empty; retry later or investigate upstream issues"
      ),
      TRUE
    )
    AND IF(
      _n_with_query < 1,
      ERROR(
        "The source partition of suggest-searches-prod-a30f.sanitized.suggest_impression_sanitized_v2 contains rows, but none have sanitized_query populated; investigate upstream issues with log routing"
      ),
      TRUE
    )
)
SELECT
  *
FROM
  validated_impressions
