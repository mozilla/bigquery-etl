{#
-- Disabled for now due to known duplication issue in Fenix data, see: 
-- [bug-1887708](https://bugzilla.mozilla.org/show_bug.cgi?id=1887708)
-- #warn
-- {{ is_unique(["client_id"]) }}
#}

#warn
{{ min_row_count(1, "submission_date = @submission_date") }}

#warn
SELECT
  IF(
    DATE_DIFF(submission_date, first_seen_date, DAY) <> 27,
    ERROR(
      "Day difference between submission_date and first_seen_date is not equal to 27 as expected"
    ),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = @submission_date;
