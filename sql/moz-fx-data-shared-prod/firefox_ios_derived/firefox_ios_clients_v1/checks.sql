#fail
{{ is_unique(columns=["client_id"]) }}
#fail
{{ not_null(columns=["client_id"]) }}
#fail
{{ min_row_count(1, "first_seen_date = @submission_date") }}
#warn
SELECT
  IF(
    (COUNTIF(is_suspicious_device_client) / COUNT(*)) * 100 > 5,
    ERROR("The % of suspicious device clients exceeds 5%"),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.firefox_ios_clients_v1`
WHERE
  first_seen_date = @submission_date;
