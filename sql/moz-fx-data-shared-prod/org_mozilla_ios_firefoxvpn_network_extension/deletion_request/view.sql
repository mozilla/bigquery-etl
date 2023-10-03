-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension.deletion_request`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.deletion_request_v1`
