#fail
{{ min_row_count(1, "submission_date = @submission_date") }}

#fail
{{ is_unique(["submission_date", "meta_attribution_app", "normalized_channel", "country"]) }}

