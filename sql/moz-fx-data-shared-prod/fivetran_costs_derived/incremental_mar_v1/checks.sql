#fail
{{ not_null(columns=["measured_date", "measured_month", "destination_id", "connector", "table_name", "billing_type", "active_rows"]) }}

#fail
{{ min_rows(1) }}
