-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.migration`
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
              metrics.datetime.migration_telemetry_identifiers_fennec_profile_creation_date
            ) AS migration_telemetry_identifiers_fennec_profile_creation_date,
            metrics.datetime.migration_telemetry_identifiers_fennec_profile_creation_date AS raw_migration_telemetry_identifiers_fennec_profile_creation_date
          ) AS datetime
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.migration_v1`
