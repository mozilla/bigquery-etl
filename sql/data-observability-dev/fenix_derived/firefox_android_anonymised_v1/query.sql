SELECT
  SHA256(client_id) AS client_id_hashed,
  first_seen_date,
  channel,
  ASCII(SHA256(client_id)) AS funky_column,
FROM
  `data-observability-dev.fenix_derived.firefox_android_clients`
WHERE
  submission_date = @submission_date
