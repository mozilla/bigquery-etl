#fail
{{ min_row_count(1000, where="submission_date = @submission_date") }}
#fail
{{ not_null(columns=["submission_date", "client_id", "first_seen_date"], where="submission_date = @submission_date") }}
#fail
{{ row_count_within_past_partitions_avg(number_of_days=7, threshold_percentage=5) }}
#fail
{{ value_length(column="client_id", expected_length=36, where="submission_date = @submission_date") }}
{#
-- Commented out due to upstream duplication issue inside Fenix data
-- which will cause this check to fail, see: bug(1803609).
-- Once the duplication issue has been resolved, this check can be uncommented.
-- #fail
-- {{ is_unique(columns=["client_id"], where="submission_date = @submission_date") }}
#}
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
  "app_version",
  "os_version_major",
  "os_version_minor",
  "os_version_patch"
], where="submission_date = @submission_date") }}
#warn
{{ value_length(column="country", expected_length=2, where="submission_date = @submission_date") }}
#warn
SELECT
  IF(
    COUNTIF(
      normalized_app_name NOT IN (
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
      )
    ) > 0,
    ERROR("Unexpected values for field normalized_app_name detected."),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = @submission_date;

#warn
SELECT
  IF(
    COUNTIF(
      activity_segment NOT IN (
        "casual_user",
        "regular_user",
        "infrequent_user",
        "other",
        "core_user"
      )
    ) > 0,
    ERROR("Unexpected values for field activity_segment detected."),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = @submission_date;

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
{{ matches_pattern(column="country", pattern="^[A-Z]{2}$", where="submission_date = @submission_date", message="Some values in this field do not adhere to the ISO 3166-1 specification (2 character country code, for example: DE).") }}
