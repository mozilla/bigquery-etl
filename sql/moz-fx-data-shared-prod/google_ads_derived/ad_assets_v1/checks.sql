#fail
{{ is_unique(["asset_id"]) }}

#fail
{{ not_null(["asset_id", "asset_type"]) }}

#fail
{{ min_row_count(1) }}
