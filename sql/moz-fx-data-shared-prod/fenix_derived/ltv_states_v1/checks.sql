#fail
{{ is_unique("client_id", "submission_date = @submission_date") }}

#fail
{{ min_row_count(10000, "submission_date = @submission_date") }}

#fail
{{ min_row_count(10000, "submission_date = @submission_date AND first_seen_date = @submission_date") }}

#fail
{{ min_row_count(10000, "submission_date = @submission_date AND days_since_seen = 0") }}
