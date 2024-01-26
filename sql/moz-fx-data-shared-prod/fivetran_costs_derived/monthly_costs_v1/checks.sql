#fail
{{ is_unique(columns=["destination_id", "measured_month"]) }}
#fail
{{ not_null(columns=["destination_id", "measured_month"]) }}
#fail
{{ min_row_count(1) }}
