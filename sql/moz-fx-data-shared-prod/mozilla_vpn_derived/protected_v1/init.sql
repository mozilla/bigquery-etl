-- Don't OR REPLACE because this table has longer retention than its source
CREATE TABLE IF NOT EXISTS
  protected_v1
AS
-- WARNING: on mobile this is undercounted and may not be measurable
SELECT
  TO_HEX(SHA256(jsonPayload.fields.fxa_uid)) AS fxa_uid,
  MIN(`timestamp`) AS first_protected,
FROM
  `moz-fx-guardian-prod-bfc7`.log_storage.stdout
WHERE
  jsonPayload.fields.isprotected
GROUP BY
  fxa_uid
