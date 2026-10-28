#fail
{{ is_unique(["`date`", "campaign_id", "ad_group_id", "ad_id", "asset_id", "field_type"], "`date` BETWEEN DATE_SUB(@submission_date, INTERVAL @lookback_days DAY) AND @submission_date") }}

#fail
{{ not_null(["`date`", "campaign_id", "ad_group_id", "ad_id", "asset_id", "field_type", "market", "performance_label"], "`date` BETWEEN DATE_SUB(@submission_date, INTERVAL @lookback_days DAY) AND @submission_date") }}

#fail
{{ accepted_values("performance_label", ["Best", "Good", "Low", "Learning", "Pending", "Unrated"], "`date` BETWEEN DATE_SUB(@submission_date, INTERVAL @lookback_days DAY) AND @submission_date") }}

#fail
{{ min_row_count(1, "`date` BETWEEN DATE_SUB(@submission_date, INTERVAL @lookback_days DAY) AND @submission_date") }}
