#fail
{{ min_row_count(1000, where="submission_date = @submission_date") }}

#fail
{{ not_null(columns=["submission_date", "client_id", "first_seen_date"], where="submission_date = @submission_date") }}

#fail
{{ row_count_within_past_partitions_avg(number_of_days=7, threshold_percentage=5) }}

#fail
{{ value_length(column="client_id", expected_length=36, where="submission_date = @submission_date") }}

#warn
{{ is_unique(columns=["client_id"], where="submission_date = @submission_date") }}

#warn
{{ not_null(columns=[
  "activity_segment",
  "normalized_app_name",
  "normalized_channel",
  "country",
  "days_seen_bits",
  "days_since_first_seen",
  "days_since_seen",
  "is_new_profile",
  "normalized_os",
  "normalized_os_version",
  "app_version",
  "os_version_major",
  "os_version_minor",
  "os_version_patch"
], where="submission_date = @submission_date") }}

#warn
{{ value_length(column="country", expected_length=2, where="submission_date = @submission_date") }}

#warn
SELECT IF(
  COUNTIF(normalized_app_name NOT IN (
    "Firefox Desktop",
    "Firefox iOS",
    "Firefox iOS BrowserStack",
    "Focus iOS",
    "Focus iOS BrowserStack",
    "Fenix",
    "Fenix BrowserStack",
    "Focus Android",
    "Focus Android Glean",
    "Focus Android Glean BrowserStack",
    "Klar iOS"
  )) > 0,
  ERROR("Unexpected values for field normalized_app_name detected."),
  null
)
FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE submission_date = @submission_date;

#warn
SELECT IF(
  COUNTIF(activity_segment NOT IN (
    "casual_user",
    "regular_user",
    "infrequent_user",
    "other",
    "core_user"
  )) > 0,
  ERROR("Unexpected values for field activity_segment detected."),
  null
)
FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE submission_date = @submission_date;

#warn
SELECT IF(
  COUNTIF(normalized_channel NOT IN (
    "nightly",
    "aurora",
    "release",
    "Other",
    "beta",
    "esr"
  )) > 0,
  ERROR("Unexpected values for field normalized_channel detected."),
  null
)
FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE submission_date = @submission_date;

#warn
SELECT IF(
  COUNTIF(NOT REGEXP_CONTAINS(CAST(country AS STRING), r"^[A-Z]{2}|\?\?$")) > 0,
  ERROR("Unexpected values for field normalized_channel detected."),
  null
)
FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE submission_date = @submission_date;
