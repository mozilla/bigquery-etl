#fail
{{ is_unique(columns=["client_id"]) }}
#fail
{{ not_null(columns=["client_id", "sample_id"]) }}
#fail
{{ min_row_count(1, "first_seen_date = @submission_date") }}
#warn
WITH base AS (
  SELECT COUNTIF(is_activated)
  FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE first_seen_date = @submission_date
),
upstream AS (
  SELECT COUNTIF(activated = 1)
  FROM `{{ project_id }}.{{ dataset_id }}.new_profile_activation_v1`
  WHERE first_seen_date = @submission_date
)
SELECT
  IF(
    (SELECT * FROM base) <> (SELECT * FROM upstream),
    ERROR(CONCAT("Number of activations does not match up that of the upstream table. Upstream count: ", (SELECT * FROM upstream), ", base count: ", (SELECT * FROM base))),
    NULL
  );
