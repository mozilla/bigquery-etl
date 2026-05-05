SELECT
  TO_HEX(SHA256(jsonPayload.fields.fxa_uid)) AS fxa_uid,
  `timestamp`,
FROM
  `moz-fx-guardian-prod-bfc7`.log_storage.stdout
WHERE
  jsonPayload.type LIKE "%.api.addDevice"
  {% if not is_init() %}
    AND DATE(`timestamp`) = @submission_date
  {% endif %}
