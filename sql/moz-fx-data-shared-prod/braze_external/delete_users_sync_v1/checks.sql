-- macro checks

#warn
{{ not_null(["EXTERNAL_ID"]) }}

#warn
{{ min_row_count(1) }}
