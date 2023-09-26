#fail
{{ is_unique(["client_id"]) }}
#fail
{{ min_row_count(1, "first_seen_date = DATE_SUB(@submission_date, INTERVAL 13 DAY)") }}

