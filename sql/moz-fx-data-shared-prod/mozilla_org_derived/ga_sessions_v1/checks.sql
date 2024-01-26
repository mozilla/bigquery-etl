#fail
-- ga_session_id should be unique across all partitions
{{ is_unique(["ga_session_id"], "session_date = @session_date") }}
#fail
{{ min_row_count(100, "session_date = @session_date") }}
