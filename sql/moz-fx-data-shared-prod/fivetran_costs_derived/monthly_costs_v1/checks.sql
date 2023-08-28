{{ is_unique(columns=["destination_id"]) }}

{{ not_null(columns=["destination_id", "measured_month"]) }}

{{ min_rows(1) }}
