SELECT
  * REPLACE (
    (
      SELECT AS STRUCT
        metadata.* REPLACE (
          -- We null out metadata.geo.city in the sanitized data to reduce the possibility
          -- of correlating low-frequency queries with a particular client based on geo.
          (SELECT AS STRUCT metadata.geo.* REPLACE (CAST(NULL AS STRING) AS city)) AS geo
        )
    ) AS metadata
  )
FROM
  contextual_services_stable.quicksuggest_impression_v1
WHERE
  DATE(submission_timestamp) = @submission_date
