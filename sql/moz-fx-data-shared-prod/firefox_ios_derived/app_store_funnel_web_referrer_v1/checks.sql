#warn
{{ is_unique(["`date`", "app_id", "territory", "web_referrer", "campaign"]) }}

#warn
{{ min_row_count(1, "`date` = @submission_date") }}
