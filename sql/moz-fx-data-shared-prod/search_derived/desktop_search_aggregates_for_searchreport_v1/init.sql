CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_derived.desktop_search_aggregates_for_searchreport_v1`(
    submission_date DATE,
    geo STRING,
    locale STRING,
    engine STRING,
    os STRING,
    app_version STRING,
    dcc INT64,
    sap INT64,
    tagged_sap INT64,
    tagged_follow_on INT64,
    search_with_ads INT64,
    ad_click INT64,
    organic INT64
  )
PARTITION BY
  submission_date
CLUSTER BY
  geo,
  locale,
  engine,
  app_version
