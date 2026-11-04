#warn
{{ is_unique(["`date`", "app_id", "territory", "source_type", "app_referrer", "web_referrer", "campaign"]) }}

#warn
{{ min_row_count(1, "`date` = @submission_date") }}
