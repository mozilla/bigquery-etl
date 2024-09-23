-- macro checks

#fail
{{ not_null(["scheduled_corpus_item_id"]) }}

#fail
{{ not_null(["impression_count"]) }}

#fail
{{ not_null(["click_count"]) }}

#fail
{{ min_row_count(1) }}
