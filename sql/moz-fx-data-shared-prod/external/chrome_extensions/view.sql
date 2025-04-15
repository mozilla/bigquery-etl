CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.chrome_extensions`
AS
SELECT
  submission_date,
  `url`,
  chrome_extension_name,
  star_rating AS star_rating_raw,
  SAFE_CAST(star_rating AS numeric) AS star_rating_numeric,
  number_of_ratings AS number_of_ratings_raw,
  CASE
    WHEN REGEXP_CONTAINS(LOWER(number_of_ratings), r'^\d+(\.\d+)?k$')
      THEN CAST(REGEXP_EXTRACT(LOWER(number_of_ratings), r'^(\d+(?:\.\d+)?)') AS FLOAT64) * 1000
    WHEN REGEXP_CONTAINS(number_of_ratings, r'^\d+$')
      THEN CAST(number_of_ratings AS INT64)
    ELSE NULL
  END AS number_of_ratings_numeric,
  number_of_users AS number_of_users_raw,
  SAFE_CAST(number_of_users AS INT64) AS number_of_users_numeric,
  extension_version,
  extension_size,
  extension_languages,
  developer_desc,
  developer_email,
  developer_website,
  developer_phone,
  extension_updated_date,
  category
FROM
  `moz-fx-data-shared-prod.external_derived.chrome_extensions_v1`
