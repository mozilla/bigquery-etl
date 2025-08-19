#warn
{{ is_unique(["ga_client_id", "ga_session_id"]) }}

#warn
{{ not_null(["ga_client_id", "ga_session_id"])}}
