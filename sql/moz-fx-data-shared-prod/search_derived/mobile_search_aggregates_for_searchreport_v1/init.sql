CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_derived.mobile_search_aggregates_for_searchreport_v1`(
    submission_date DATE,
    country STRING,
    product STRING,
    normalized_engine STRING,
    clients INT64,
    search_clients INT64,
    sap INT64,
    tagged_sap INT64,
    tagged_follow_on INT64,
    ad_click INT64,
    search_with_ads INT64,
    organic INT64
  )
PARTITION BY
  submission_date
CLUSTER BY
  country,
  product,
  normalized_engine
