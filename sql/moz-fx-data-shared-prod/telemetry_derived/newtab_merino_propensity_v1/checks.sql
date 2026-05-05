-- macro checks

#fail
{{ not_null(["weight"]) }}

#fail
{{ min_row_count(1) }}
