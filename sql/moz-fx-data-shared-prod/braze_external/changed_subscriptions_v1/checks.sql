-- macro checks

#warn
{{ not_null(["external_id"]) }}

#warn
{{ min_row_count(1) }}

#warn
{{ is_unique(["external_id"]) }}
