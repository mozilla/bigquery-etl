SELECT
  SHA256(client_id) AS client_id_hashed,
  first_seen_date,
  channel,
  CAST(ASCII(SHA256(client_id)) AS STRING) AS funky_column,
FROM
  `data-observability-dev.fenix.firefox_android_clients`
WHERE
  submission_date = @submission_date
