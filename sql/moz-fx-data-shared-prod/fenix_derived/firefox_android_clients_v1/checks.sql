#warn
{{ is_unique(columns=["client_id"]) }}

#warn
{{ not_null(columns=["client_id", "sample_id"], where="submission_date = @submission_date") }}

#fail
{{ min_row_count(1, "submission_date = @submission_date") }}
