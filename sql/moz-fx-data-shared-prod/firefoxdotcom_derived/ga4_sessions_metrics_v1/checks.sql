#fail
{{ min_row_count(1, "session_date = @submission_date") }}

#fail
{{ not_null(["session_date"], "session_date = @submission_date") }}

#fail
{{ is_unique(["session_date", "hostname", "landing_locale", "landing_page", "landing_page_type", "country", "device_category", "source", "medium", "campaign", "channel_group", "ad_platform"], "session_date = @submission_date") }}

#warn
{{ row_count_within_past_partitions_avg(7, 50) }}
