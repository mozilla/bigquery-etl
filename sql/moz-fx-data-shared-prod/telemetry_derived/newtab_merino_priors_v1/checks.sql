-- macro checks

#fail
{{ not_null(["average_ctr_top2_items"]) }}

#fail
{{ not_null(["average_ctr_top10_items"]) }}

#fail
{{ not_null(["impressions_per_item"]) }}

#fail
{{ not_null(["total_impressions_per_day"]) }}

#fail
{{ min_row_count(1) }}
