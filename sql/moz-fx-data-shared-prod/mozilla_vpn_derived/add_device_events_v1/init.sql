-- Don't OR REPLACE because this table has longer retention than its sources
CREATE TABLE IF NOT EXISTS
  mozilla_vpn_derived.add_device_events_v1
PARTITION BY
  DATE(`timestamp`)
AS
SELECT
  TO_HEX(SHA256(jsonPayload.fields.fxa_uid)) AS fxa_uid,
  `timestamp`,
FROM
  `moz-fx-guardian-prod-bfc7`.log_storage.stdout
WHERE
  jsonPayload.type LIKE "%.api.addDevice"
