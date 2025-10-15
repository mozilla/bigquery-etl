{{ header }}

{% raw %}
{% if is_init() %}
{% endraw %}

WITH eventsstream AS (
  SELECT
    client_id,
    profile_group_id,
    sample_id,
    event_category,
    event_name,
    `event`,
    CAST(NULL AS string) AS criteria,
    MIN(submission_timestamp) AS first_submission_timestamp,
    MIN(event_timestamp) AS first_event_timestamp,
    min_by(event_extra, submission_timestamp) AS event_extra,
    min_by(app_version_major, submission_timestamp) AS app_version_major,
    min_by(normalized_channel, submission_timestamp) AS normalized_channel,
    min_by(normalized_country_code, submission_timestamp) AS normalized_country_code,
    min_by(normalized_os, submission_timestamp) AS normalized_os,
    min_by(normalized_os_version, submission_timestamp) AS normalized_os_version,
  FROM
    `{{ project_id }}.{{ app_name }}_derived.events_stream_v1`
  WHERE
    -- initialize by looking over all of history
    DATE(submission_timestamp) >= '2023-01-01'
    AND profile_group_id IS NOT NULL
    AND event_category NOT IN ('media.playback', 'nimbus_events', 'uptake.remotecontent.result')
    AND sample_id = @sample_id
  GROUP BY
    client_id,
    profile_group_id,
    sample_id,
    event_category,
    event_name,
    `event`,
    criteria
)
SELECT
  *
FROM
  eventsstream

{% raw %}
{% else %}
{% endraw %}

WITH _current AS (
  SELECT
    @submission_date AS submission_date,
    @submission_date AS event_first_seen_date,
    client_id,
    profile_group_id,
    sample_id,
    event_category,
    event_name,
    `event`,
    CAST(NULL AS string) AS criteria,
    MIN(submission_timestamp) AS first_submission_timestamp,
    MIN(event_timestamp) AS first_event_timestamp,
    min_by(event_extra, submission_timestamp) AS event_extra,
    min_by(app_version_major, submission_timestamp) AS app_version_major,
    min_by(normalized_channel, submission_timestamp) AS normalized_channel,
    min_by(normalized_country_code, submission_timestamp) AS normalized_country_code,
    min_by(normalized_os, submission_timestamp) AS normalized_os,
    min_by(normalized_os_version, submission_timestamp) AS normalized_os_version,
  FROM
    `{{ project_id }}.{{ app_name }}_derived.events_stream_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND profile_group_id IS NOT NULL
    AND event_category NOT IN ('media.playback', 'nimbus_events', 'uptake.remotecontent.result')
  GROUP BY
    submission_date,
    event_first_seen_date,
    client_id,
    profile_group_id,
    sample_id,
    event_category,
    event_name,
    `event`,
    criteria
),
-- query over all of history to see whether the client_id has shown up before
_previous AS (
  SELECT
    submission_date,
    event_first_seen_date,
    client_id,
    profile_group_id,
    sample_id,
    event_category,
    event_name,
    `event`,
    CAST(NULL AS string) AS criteria,
    first_submission_timestamp,
    first_event_timestamp,
    event_extra,
    app_version_major,
    normalized_channel,
    normalized_country_code,
    normalized_os,
    normalized_os_version
  FROM
    `{{ project_id }}.{{ events_first_seen_table }}`
  WHERE
    event_first_seen_date > '2023-01-01'
    AND event_first_seen_date < @submission_date
),
_joined AS (
  --switch to using separate if statements instead of 1
  --because dry run is struggling to validate the final struct
  SELECT
    IF(
      _previous.client_id IS NULL
      OR _previous.event_first_seen_date >= _current.event_first_seen_date,
      _current,
      _previous
    ).*
  FROM
    _current
  FULL JOIN
    _previous
    USING (client_id)
)
SELECT
  *
FROM
  _joined

from _joined

{% raw %}
{% endif %}
{% endraw %}
