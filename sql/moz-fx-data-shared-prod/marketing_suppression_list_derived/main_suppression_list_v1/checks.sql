#fail
{{ not_null(["email", "suppressed_timestamp"]) }}

#fail
{{ min_row_count(1) }}

#warn
{{ is_unique(["email"]) }}
