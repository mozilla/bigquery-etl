#fail
{{ is_unique(columns=["destination", "measured_date", "connector", "billing_type"]) }}
#fail
{{ not_null(columns=["destination", "measured_date", "connector", "billing_type", "active_rows", "cost_in_usd"]) }}
#fail
{{ min_row_count(1) }}
