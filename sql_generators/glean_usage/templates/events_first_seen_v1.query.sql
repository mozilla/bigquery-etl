{{ header }}

{% for criteria_name, criteria_sql in criteria.items() %}

(
  WITH events_stream_cte AS (
    SELECT
      MIN(submission_timestamp) AS first_submission_timestamp,
      client_id,
      `event`,
      event_category,
      event_name,
      {{ ("'" ~ criteria_name ~ "'") }} AS criteria,
      ARRAY_AGG(
        STRUCT(
          profile_group_id,
          sample_id,
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
          COALESCE(event_timestamp, '9999-12-31 23:59:59'),
          document_event_number
        LIMIT
          1
      )[0].*
    FROM
      `{{ project_id }}.{{ app_id_dataset }}_derived.events_stream_v1`
    WHERE
      {% raw %}{% if is_init() %}{% endraw %}
        DATE(submission_timestamp) >= '2023-01-01'  -- initialize by looking over all of history
        AND sample_id >= @sample_id
        AND sample_id < @sample_id + @sampling_batch_size
      {% raw %}{% else %}{% endraw %}
        DATE(submission_timestamp) = @submission_date
      {% raw %}{% endif %}{% endraw %}
      -- remove unnecessary high-volume categories to reduce cost
      AND event_category NOT IN (
        'media.playback',
        'nimbus_events',
        'uptake.remotecontent.result'
      )
      {% if app_id_dataset == 'firefox_desktop' -%}
      AND profile_group_id IS NOT NULL -- only include non-null IDs so as not to create repeats
      {% endif %}
        -- below is the templated criteria
      AND ({{ criteria_sql }})
    GROUP BY
      client_id,
      `event`,
      event_category,
      event_name,
      criteria
  )
  {% raw %}
  {% if is_init() %}
  {% endraw %}
  SELECT
    *
  FROM
    events_stream_cte
  {% raw %}
  {% else %}
  {% endraw %}
    -- query over all of history to see whether the client_id, event and criteria combination has shown up before
  ,
  _previous_cte AS (
    SELECT
      first_submission_timestamp,
      client_id,
      `event`,
      event_category,
      event_name,
      criteria,
      profile_group_id,
      sample_id,
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
      DATE(first_submission_timestamp) >= '2023-01-01'
      AND DATE(first_submission_timestamp) < @submission_date
      AND criteria != {{ "'" ~ criteria_name ~ "'" }}
  )
  SELECT
    events_stream_cte.*
  FROM
    events_stream_cte
  LEFT JOIN
    _previous_cte
    ON events_stream_cte.client_id = _previous_cte.client_id
    AND events_stream_cte.event = _previous_cte.event
    AND events_stream_cte.criteria = _previous_cte.criteria
  WHERE
    _previous_cte.client_id IS NULL
    {% raw %}
    {% endif %}
    {% endraw %}
)

{% if not loop.last -%}
UNION ALL
{% endif %}
{% endfor %}
