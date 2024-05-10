-- macro checks

#warn
{{ not_null(["EXTERNAL_ID"]) }}

#warn
{{ min_row_count(1) }}

#warn
{{ is_unique(["EXTERNAL_ID"]) }}
