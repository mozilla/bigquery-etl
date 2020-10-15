SELECT
  TO_HEX(SHA256(jsonPayload.fields.fxa_uid)) AS fxa_uid,
  `timestamp`,
FROM
  `moz-fx-guardian-prod-bfc7`.log_storage.stdout
WHERE
  jsonPayload.type LIKE "%.api.addDevice"
  AND DATE(`timestamp`) = @submission_date
