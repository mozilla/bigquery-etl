CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_aggregates` AS
SELECT
  submission_date_s3,
  submission_date_s3 AS submission_date,
  app_version,
  country,
  distribution_id,
  engine,
  locale,
  search_cohort,
  source,
  default_search_engine,
  IFNULL(organic, 0) AS organic,
  IFNULL(tagged_sap, 0) AS tagged_sap,
  IFNULL(tagged_follow_on, 0) AS tagged_follow_on,
  IFNULL(sap, 0) AS sap,
  IFNULL(ad_click, 0) AS ad_click,
  IFNULL(search_with_ads, 0) AS search_with_ads,
  IFNULL(unknown, 0) AS unknown,
  addon_version,
  os,
  os_version,
  IFNULL(client_count, 0) AS client_count,
  NULL AS default_private_search_engine,
  NULL AS normalized_engine
FROM
  `moz-fx-data-derived-datasets.search.search_aggregates_v6`
WHERE
 submission_date_s3 >= '2019-05-04'
 AND submission_date_s3 <= '2019-05-11'
UNION ALL
SELECT
  submission_date AS submission_date_s3,
  *
FROM
  `moz-fx-data-shared-prod.search_derived.search_aggregates_v8`
WHERE
  submission_date < '2019-05-04'
  OR submission_date > '2019-05-11'
