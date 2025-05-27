#fail
{{ is_unique(["client_id"], "submission_date = @submission_date")}}

#fail
{{ min_row_count(1, "submission_date = @submission_date") }}
