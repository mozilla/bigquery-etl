#fail
{{ is_unique(columns=["client_id"]) }}

#fail
{{ not_null(columns=["client_id", "sample_id"], where="submission_date = @submission_date") }}

#fail
{{ min_row_count(1, "submission_date = @submission_date") }}

#warn
WITH base AS (
  SELECT
    COUNTIF(activated)
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    first_seen_date = @submission_date
),
upstream AS (
  SELECT
    COUNTIF(activated = 1)
  FROM
    `{{ project_id }}.{{ dataset_id }}.new_profile_activation_v1`
  WHERE
    first_seen_date = @submission_date
    AND submission_date = DATE_SUB(@submission_date, INTERVAL 6 DAY)
)
SELECT
  IF(
    (SELECT * FROM base) <> (SELECT * FROM upstream),
    ERROR(
      CONCAT(
        "Number of activations does not match up that of the upstream table. Upstream count: ",
        (SELECT * FROM upstream),
        ", base count: ",
        (SELECT * FROM base)
      )
    ),
    NULL
  );
