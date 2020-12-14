CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_derived.desktop_search_aggregates_by_userstate_v1`(
    submission_date DATE,
    geo STRING,
    user_state STRING,
    client_count INT64,
    search_client_count INT64,
    sap INT64,
    search_with_ads INT64,
    ad_clicks INT64,
    tagged_follow_on INT64,
    tagged_sap INT64,
    organic INT64
  )
PARTITION BY
  submission_date
CLUSTER BY
  geo,
  user_state
