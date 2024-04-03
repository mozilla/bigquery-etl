-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pine.spoc`
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
              metrics.datetime.pocket_fetch_timestamp
            ) AS pocket_fetch_timestamp,
            metrics.datetime.pocket_fetch_timestamp AS raw_pocket_fetch_timestamp,
            mozfun.glean.parse_datetime(
              metrics.datetime.pocket_newtab_creation_timestamp
            ) AS pocket_newtab_creation_timestamp,
            metrics.datetime.pocket_newtab_creation_timestamp AS raw_pocket_newtab_creation_timestamp
          ) AS datetime
        ),
        metrics.text2 AS text
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.pine_stable.spoc_v1`
