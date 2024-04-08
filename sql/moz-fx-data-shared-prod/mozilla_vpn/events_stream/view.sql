-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.events_stream`
AS
SELECT
  "mozillavpn" AS normalized_app_id,
  e.* REPLACE ("release" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.mozillavpn.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_firefox_vpn" AS normalized_app_id,
  e.* REPLACE ("release" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn" AS normalized_app_id,
  e.* REPLACE ("release" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn_network_extension" AS normalized_app_id,
  e.* REPLACE ("release" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension.events_stream` AS e
