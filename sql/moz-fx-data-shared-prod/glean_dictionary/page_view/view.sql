-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_dictionary.page_view`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (
      SELECT AS STRUCT
        metrics.* REPLACE (
          STRUCT(
            mozfun.glean.parse_datetime(metrics.datetime.page_loaded) AS page_loaded,
            metrics.datetime.page_loaded AS raw_page_loaded
          ) AS datetime
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.glean_dictionary_stable.page_view_v1`
