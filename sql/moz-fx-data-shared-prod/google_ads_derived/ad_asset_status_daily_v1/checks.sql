#fail
{{ is_unique(["`date`", "campaign_id", "ad_group_id", "ad_id", "asset_id", "field_type"], "`date` = @submission_date") }}

#fail
{{ not_null(["`date`", "campaign_id", "ad_group_id", "ad_id", "asset_id", "field_type", "market", "performance_label", "is_live"], "`date` = @submission_date") }}

#fail
{{ accepted_values("performance_label", ["Best", "Good", "Low", "Learning", "Pending", "Unrated"], "`date` = @submission_date") }}

#fail
{{ min_row_count(1, "`date` = @submission_date") }}

#warn
{{ not_null(["campaign_status", "ad_group_status"], "`date` = @submission_date") }}
