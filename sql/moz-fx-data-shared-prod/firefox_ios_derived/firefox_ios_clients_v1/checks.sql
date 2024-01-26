#fail
{{ is_unique(columns=["client_id"]) }}

#fail
{{ not_null(columns=["client_id"], where="first_seen_date = @submission_date") }}

#fail
{{ min_row_count(1, where="first_seen_date = @submission_date") }}

#warn
SELECT
  IF(
    (COUNTIF(is_suspicious_device_client) / COUNT(*)) * 100 > 5,
    ERROR("The % of suspicious device clients exceeds 5%"),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  first_seen_date = @submission_date;
