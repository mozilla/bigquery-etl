{{ is_unique(columns=["destination", "measured_date", "connector", "billing_type"]) }}
 {{ not_null(columns=["destination", "measured_date", "connector", "billing_type", "active_rows", "cost_in_usd"]) }}
 {{ min_rows(1) }}

