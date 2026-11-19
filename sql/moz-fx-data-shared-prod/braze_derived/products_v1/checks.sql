-- macro checks

#fail
{{ not_null(["external_id"]) }}

#fail
{{ min_row_count(1) }}
