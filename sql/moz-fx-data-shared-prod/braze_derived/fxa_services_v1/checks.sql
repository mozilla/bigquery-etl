-- macro checks

#fail
{{ not_null(["uid"]) }}

#fail
{{ min_row_count(1) }}

#warn
{{ is_unique(["uid", "service"]) }}
