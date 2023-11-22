#fail
-- Grain is: client_id
{{ is_unique(["client_id"]) }}
#fail
{{ min_row_count(100000) }}

