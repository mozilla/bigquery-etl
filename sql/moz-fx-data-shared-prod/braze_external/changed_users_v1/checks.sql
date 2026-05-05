-- macro checks

#warn
{{ not_null(["external_id", "status", "email", "create_timestamp", "update_timestamp"]) }}

#warn
{{ min_row_count(1) }}

#warn
{{ is_unique(["external_id", "email"]) }}
