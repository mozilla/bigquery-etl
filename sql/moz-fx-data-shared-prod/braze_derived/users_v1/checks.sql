-- macro checks

#fail
{{ not_null(["external_id", "email", "email_subscribe", "has_fxa", "create_timestamp", "update_timestamp"]) }}

#fail
{{ min_row_count(85000000) }}

#fail
{{ is_unique(["external_id", "email", "fxa_id_sha256"]) }}
