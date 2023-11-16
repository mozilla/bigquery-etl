#fail
-- ga_session_id should be unique across all partitions
{{ is_unique(["ga_session_id"]) }}

#fail
{{ min_row_count(10000) }}

