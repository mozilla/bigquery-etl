{{ header }}

{% raw %}
{% if is_init() %}
{% endraw %}

{% for item in criteria %}

(
WITH eventsstream AS (
  SELECT
  DATE(MIN(submission_timestamp)) as submission_date,
  DATE(MIN(submission_timestamp)) as event_first_seen_date,
  client_id,
  `event`,
  event_category,
  event_name,
  {{ item["name"] }} AS criteria,
  ARRAY_AGG(
    STRUCT(
      profile_group_id,
      sample_id,
      submission_timestamp AS first_submission_timestamp,
      event_timestamp AS first_event_timestamp,
      event_extra,
      app_version_major,
      normalized_channel,
      normalized_country_code,
      normalized_os,
      normalized_os_version,
      client_info.windows_build_number
    )
    ORDER BY
      submission_timestamp,
      COALESCE(event_timestamp, '9999-12-31 23:59:59')
    LIMIT 1
  )[0].*
  FROM
  `{{ project_id }}.{{ app_id }}_derived.events_stream_v1`
  WHERE
        -- initialize by looking over all of history
  DATE(submission_timestamp) >= '2023-01-01'
  AND sample_id >= @sample_id
  AND sample_id < @sample_id + @sampling_batch_size
  AND event_category NOT IN ('media.playback', 'nimbus_events', 'uptake.remotecontent.result')
        -- if app_id is firefox_desktop, filter for where profile_group_id is not null
  {% if app_id == 'firefox_desktop' -%}
  AND profile_group_id IS NOT NULL
  {% endif %}
        -- below is the templated criteria
  AND ({{ item["sql"] }})
  GROUP BY
  client_id,
  `event`,
  event_category,
  event_name,
  criteria
  )
SELECT
*
FROM
eventsstream
)

{% if not loop.last -%}
UNION ALL
{% endif %}
{% endfor %}

{% raw %}
{% else %}
{% endraw %}

{% for item in criteria %}

(
WITH _current AS (
  SELECT
    @submission_date AS submission_date,
    @submission_date AS event_first_seen_date,
    client_id,
    `event`,
    event_category,
    event_name,
    {{ item["name"] }} AS criteria,
    ARRAY_AGG(
        STRUCT(
          profile_group_id,
          sample_id,
          submission_timestamp AS first_submission_timestamp,
          event_timestamp AS first_event_timestamp,
          event_extra,
          app_version_major,
          normalized_channel,
          normalized_country_code,
          normalized_os,
          normalized_os_version,
          client_info.windows_build_number
        )
        ORDER BY
          submission_timestamp,
          COALESCE(event_timestamp, '9999-12-31 23:59:59')
        LIMIT 1
      )[0].*
  FROM
    `{{ project_id }}.{{ app_id }}_derived.events_stream_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category NOT IN ('media.playback', 'nimbus_events', 'uptake.remotecontent.result')
        -- if app_id is firefox_desktop, filter for where profile_group_id is not null
    {% if app_id == 'firefox_desktop' -%}
    AND profile_group_id IS NOT NULL
    {% endif %}
        -- below is the templated criteria
    AND ({{ item["sql"] }})
  GROUP BY
    submission_date,
    event_first_seen_date,
    client_id,
    `event`,
    event_category,
    event_name,
    criteria
    ),
  -- query over all of history to see whether the client_id, event and criteria combination has shown up before
  _previous AS (
    SELECT
      submission_date,
      event_first_seen_date,
      client_id,
      `event`,
      event_category,
      event_name,
      {{ item["name"] }} AS criteria,
      profile_group_id,
      sample_id,
      first_submission_timestamp,
      first_event_timestamp,
      event_extra,
      app_version_major,
      normalized_channel,
      normalized_country_code,
      normalized_os,
      normalized_os_version,
      windows_build_number
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
    FULL OUTER JOIN
      _previous
      ON _current.client_id = _previous.client_id
          AND _current.event = _previous.event
          AND (_current.criteria = _previous.criteria
              OR (_current.criteria IS NULL AND _previous.criteria IS NULL))
  )
SELECT
  *
FROM
  _joined
)
{% if not loop.last -%}
UNION ALL
{% endif %}
{% endfor %}

{% raw %}
{% endif %}
{% endraw %}
