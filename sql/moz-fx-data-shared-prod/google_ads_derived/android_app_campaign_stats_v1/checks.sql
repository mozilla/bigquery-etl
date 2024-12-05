#fail
{{ is_unique(["date", "campaign", "ad_group"], "date = DATE_SUB(@submission_date, INTERVAL 27 DAY)") }}
