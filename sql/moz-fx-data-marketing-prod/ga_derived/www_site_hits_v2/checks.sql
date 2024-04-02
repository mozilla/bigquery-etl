#warn
{{ matches_pattern(column="visit_identifier", pattern="^[0-9]+\\.{1}[0-9]+\\-{1}[0-9]+$", where="date = @submission_date", threshold_fail_percentage=0, message="Warn - some visit_identifier not matching expected pattern") }}
