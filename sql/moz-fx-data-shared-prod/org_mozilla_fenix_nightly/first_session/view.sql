-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.first_session`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (
      SELECT AS STRUCT
        metrics.* EXCEPT (jwe, labeled_rate, text, url) REPLACE(
          STRUCT(
            mozfun.glean.parse_datetime(
              metrics.datetime.first_session_timestamp
            ) AS first_session_timestamp,
            metrics.datetime.first_session_timestamp AS raw_first_session_timestamp
          ) AS datetime
        ),
        metrics.text2 AS text
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.first_session_v1`
