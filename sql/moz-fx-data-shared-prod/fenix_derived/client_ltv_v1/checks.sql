#fail
{{ is_unique("client_id") }}

#fail
{{ min_row_count(10000, "as_of_date = @submission_date") }}
