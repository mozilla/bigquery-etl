-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.events_stream`
AS
WITH events_stream_union AS (
  SELECT
    "mozillavpn" AS normalized_app_id,
    e.* REPLACE ("release" AS normalized_channel),
  FROM
    `moz-fx-data-shared-prod.mozillavpn_derived.events_stream_v1` AS e
  UNION ALL
    BY NAME
  SELECT
    "org_mozilla_firefox_vpn" AS normalized_app_id,
    e.* REPLACE ("release" AS normalized_channel),
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_derived.events_stream_v1` AS e
  UNION ALL
    BY NAME
  SELECT
    "org_mozilla_ios_firefoxvpn" AS normalized_app_id,
    e.* REPLACE ("release" AS normalized_channel),
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_derived.events_stream_v1` AS e
  UNION ALL
    BY NAME
  SELECT
    "org_mozilla_ios_firefoxvpn_network_extension" AS normalized_app_id,
    e.* REPLACE ("release" AS normalized_channel),
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_derived.events_stream_v1` AS e
)
SELECT
  *,
FROM
  events_stream_union
