{{ header }}

{#
   We use raw here b/c the first pass is rendered to create the checks.sql
   files, and the second pass is rendering of the checks themselves.

   For example, the header above is rendered for every checks file
   when we create the checks file, when `bqetl generate glean_usage`
   is called.

   However the second part, where we render the check is_unique() below,
   is rendered when we _run_ the check, during `bqetl query backfill`
   (you can also run them locally with `bqetl check run`).
#}
{% raw -%}
#warn
{{ is_unique(["client_id"], where="submission_date = @submission_date") }}

#warn
{{ min_row_count(1, where="submission_date = @submission_date") }}

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
{{ matches_pattern(column="country", pattern="^[A-Z]{2}$", where="submission_date = @submission_date") }}

#warn
{{ value_length(column="client_id", expected_length=36, where="submission_date = @submission_date") }}

#fail
WITH daily AS
(
 SELECT
     submission_date,
     COUNT(DISTINCT client_id) AS client_count
 FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.baseline_clients_daily_v1`
 WHERE
    submission_date = @submission_date
    AND sample_id IS NOT NULL
 GROUP BY submission_date
)
,last_seen AS
(
 SELECT
  submission_date,
  COUNT(DISTINCT client_id) AS client_count
 FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.baseline_clients_last_seen_v1`
 WHERE
  submission_date = @submission_date
  AND mozfun.bits28.days_since_seen(days_seen_bits) = 0
  GROUP BY submission_date
)
,check_results AS
(
 SELECT
   1-(last_seen.client_count/daily.client_count) AS difference_perc
 FROM daily LEFT JOIN last_seen
 USING(submission_date)
)
SELECT
 IF(
 ABS((SELECT difference_perc FROM check_results)) > 0.001,
 ERROR(
   CONCAT("Results don't match by > 1%, baseline_clients_daily table has ",
   STRING(((SELECT submission_date FROM daily))),
   ": ",
   ABS((SELECT client_count FROM daily)),
   ". baseline_clients_last_seen has ",
   IFNULL(((SELECT client_count FROM last_seen)), 0)
   )
 ),
 NULL
 );

{% endraw %}
