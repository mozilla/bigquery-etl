#fail
{{ is_unique("client_id", "submission_date = @submission_date") }}

#fail
{{ min_row_count(10000, "submission_date = @submission_date") }}

#warn
{{ min_row_count(1000, "submission_date = @submission_date AND activated = 1") }}

