-- Query generated via `mobile_kpi_support_metrics` SQL generator.
{% set _WHERE = '@submission_date = @submission_date' %}

#warn
{{ min_row_count(1, where=_WHERE) }}
{#
Removed the not_null check for now due to the check failing
with "first_see_date" due to a known issue where some clients have first_seen_date in Fenix
set to null: https://bugzilla.mozilla.org/show_bug.cgi?id=1900084
The check will be readded once the core issue is resolved.
#warn
{{ not_null(["first_seen_date", "app_name", "normalized_channel", "is_mobile"], where=_WHERE) }}
#}

#warn
{{ value_length(column="country", expected_length="2", where=_WHERE) }}

#warn
{{ accepted_values(column="normalized_channel", values=["release", "beta", "nightly"], where=_WHERE) }}
