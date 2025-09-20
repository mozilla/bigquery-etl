{{ header }}

{% raw %}
{% if is_init() %}
{% endraw %}
with
eventsstream as (
    select
    client_id,
    profile_group_id,
    sample_id,
    event_category,
    event_name,
    `event`,
    cast(null as string) as criteria,
    min(submission_timestamp) as first_submission_timestamp,
    min(event_timestamp) as first_event_timestamp,
    min_by(event_extra, submission_timestamp) as event_extra,
    min_by(app_version_major, submission_timestamp) as app_version_major,
    min_by(normalized_channel, submission_timestamp) as normalized_channel,
    min_by(normalized_country_code, submission_timestamp) as normalized_country_code,
    min_by(normalized_os, submission_timestamp) as normalized_os,
    min_by(normalized_os_version, submission_timestamp) as normalized_os_version,
    from
    `{{ project_id }}.{{ app_name }}_derived.{{ base_table }}`
    where
    -- initialize by looking over all of history
    date(submission_timestamp) >= date_sub(@submission_date, interval 1 day)
    and profile_group_id is not null
    group by
    client_id,
    profile_group_id,
    sample_id,
    event_category,
    event_name,
    `event`,
    criteria
)
select * from eventsstream

{% raw %}
{% else %}
{% endraw %}

with
_current as (
    select
    @submission_date as submission_date,
    @submission_date as first_seen_date,
    client_id,
    profile_group_id,
    sample_id,
    event_category,
    event_name,
    `event`,
    cast(null as string) as criteria,
    min(submission_timestamp) as first_submission_timestamp,
    min(event_timestamp) as first_event_timestamp,
    min_by(event_extra, submission_timestamp) as event_extra,
    min_by(app_version_major, submission_timestamp) as app_version_major,
    min_by(normalized_channel, submission_timestamp) as normalized_channel,
    min_by(normalized_country_code, submission_timestamp) as normalized_country_code,
    min_by(normalized_os, submission_timestamp) as normalized_os,
    min_by(normalized_os_version, submission_timestamp) as normalized_os_version,
    from
    `{{ project_id }}.{{ app_name }}_derived.{{ base_table }}`
    where
    date(submission_timestamp) = @submission_date
    and profile_group_id is not null
    group by
    submission_date,
    first_seen_date,
    client_id,
    profile_group_id,
    sample_id,
    event_category,
    event_name,
    `event`,
    criteria
),
-- query over all of history to see whether the client_id has shown up before
_previous as (
    select
    submission_date,
    first_seen_date,
    client_id,
    profile_group_id,
    sample_id,
    event_category,
    event_name,
    `event`,
    cast(null as string) as criteria,
    first_submission_timestamp,
    first_event_timestamp,
    event_extra,
    app_version_major,
    normalized_channel,
    normalized_country_code,
    normalized_os,
    normalized_os_version
    from
    `{{ project_id }}.{{ app_name }}_derived.{{ events_first_seen_table }}` -- events_first_seen_table doesn't yet exist
    where
    first_seen_date > '2023-01-01'
    and first_seen_date < @submission_date
),
_joined as (
  --switch to using separate if statements instead of 1
  --because dry run is struggling to validate the final struct
  select
    if(
      _previous.client_id is null
      or _previous.first_seen_date >= _current.first_seen_date,
      _current,
      _previous
    ).*
  from
    _current
  full join
    _previous
    using (client_id)
)

select
*
from _joined

{% raw %}
{% endif %}
{% endraw %}
