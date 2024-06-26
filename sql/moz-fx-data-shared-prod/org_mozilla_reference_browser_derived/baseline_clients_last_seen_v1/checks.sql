-- Overrides bigquery_etl.glean_usage due to this app having very limited data, sometimes none

#warn
{{ is_unique(["client_id"], where="submission_date = @submission_date") }}

#warn
{{ not_null([
  "submission_date",
  "client_id",
  "sample_id",
  "first_seen_date",
  "days_seen_bits",
  "days_active_bits",
  "days_created_profile_bits",
  "days_seen_session_start_bits",
  "days_seen_session_end_bits"
  ], where="submission_date = @submission_date") }}

#warn
SELECT
  IF(
    COUNTIF(normalized_channel NOT IN ("nightly", "aurora", "release", "Other", "beta", "esr")) > 0,
    ERROR("Unexpected values for field normalized_channel detected."),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = @submission_date;

#warn
{{ value_length(column="client_id", expected_length=36, where="submission_date = @submission_date") }}
