#fail
{{ is_unique(["client_id"]) }}
#fail
{{ min_row_count(1, "submission_date = @submission_date") }}

