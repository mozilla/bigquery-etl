#fail
{{ not_null(["submission_date", "os"], "submission_date = @submission_date") }}

#fail
{{ min_row_count(1, "submission_date = @submission_date") }}

#fail
{{ is_unique(["submission_date", "os", "country"], "submission_date = @submission_date")}}

#fail
{{ in_range(["non_ssl_loads", "ssl_loads", "reporting_ratio"], 0, none, "submission_date = @submission_date") }}
