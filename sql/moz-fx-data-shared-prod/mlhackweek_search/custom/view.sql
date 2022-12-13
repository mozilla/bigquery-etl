-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mlhackweek_search.custom`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (
      SELECT AS STRUCT
        metrics.* REPLACE (
          STRUCT(
            mozfun.glean.parse_datetime(
              metrics.datetime.search_url_select_timestamp
            ) AS search_url_select_timestamp,
            metrics.datetime.search_url_select_timestamp AS raw_search_url_select_timestamp
          ) AS datetime
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.mlhackweek_search_stable.custom_v1`
