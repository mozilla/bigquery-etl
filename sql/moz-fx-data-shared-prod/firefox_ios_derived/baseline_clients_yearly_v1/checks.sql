#fail
{{ is_unique(["client_id"], "submission_date = @submission_date") }}

#fail
-- Should have a partition with rows for each date
{{ min_row_count(10000, "submission_date = @submission_date") }}

#fail
-- Should have clients who were active on that date present
{{ min_row_count(10000, "days_since_seen = 0 AND submission_date = @submission_date") }}

#fail
-- Should have a bunch of new profiles each date
{{ min_row_count(10000, "first_seen_date = @submission_date AND submission_date = @submission_date") }}

