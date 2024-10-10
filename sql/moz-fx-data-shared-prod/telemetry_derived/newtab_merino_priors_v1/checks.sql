-- macro checks

#fail
{{ not_null(["average_ctr_top2_items"]) }}

#fail
{{ not_null(["impressions_per_item"]) }}

#fail
{{ min_row_count(1) }}
