#warn
{{ is_unique(["ga_client_id", "ga_session_id"]) }}

#warn
{{ not_null(["session_date", "ga_client_id", "ga_session_id"])}}
