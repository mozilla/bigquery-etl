-- Generated via bigquery_etl.glean_usage
WITH events_stream_cte AS (
  (
    SELECT
      MIN(submission_timestamp) AS first_submission_timestamp,
      client_id,
      `event`,
      event_category,
      event_name,
      'any_event' AS criteria,
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
      `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
    WHERE
      {% if is_init() %}
        DATE(submission_timestamp) >= '2023-01-01'  -- initialize by looking over all of history
        AND sample_id >= @sample_id
        AND sample_id < @sample_id + @sampling_batch_size
      {% else %}
        DATE(submission_timestamp) = @submission_date
      {% endif %}
      -- remove unnecessary high-volume categories to reduce cost
      AND event_category NOT IN ('media.playback', 'nimbus_events', 'uptake.remotecontent.result')
      AND profile_group_id IS NOT NULL -- only include non-null IDs so as not to create repeats
        -- below is the templated criteria
      AND (TRUE)
    GROUP BY
      client_id,
      `event`,
      event_category,
      event_name,
      criteria
  )
  UNION ALL
    (
      SELECT
        MIN(submission_timestamp) AS first_submission_timestamp,
        client_id,
        `event`,
        event_category,
        event_name,
        'chatbot_usage' AS criteria,
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
        `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
      WHERE
        {% if is_init() %}
          DATE(submission_timestamp) >= '2023-01-01'  -- initialize by looking over all of history
          AND sample_id >= @sample_id
          AND sample_id < @sample_id + @sampling_batch_size
        {% else %}
          DATE(submission_timestamp) = @submission_date
        {% endif %}
      -- remove unnecessary high-volume categories to reduce cost
        AND event_category NOT IN ('media.playback', 'nimbus_events', 'uptake.remotecontent.result')
        AND profile_group_id IS NOT NULL -- only include non-null IDs so as not to create repeats
        -- below is the templated criteria
        AND (
          event_category = 'genai.chatbot'
          AND event_name = 'sidebar_toggle'
          AND JSON_VALUE(event_extra.provider) <> 'none'
        )
      GROUP BY
        client_id,
        `event`,
        event_category,
        event_name,
        criteria
    )
  UNION ALL
    (
      SELECT
        MIN(submission_timestamp) AS first_submission_timestamp,
        client_id,
        `event`,
        event_category,
        event_name,
        'smart_tabgroup_save' AS criteria,
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
        `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
      WHERE
        {% if is_init() %}
          DATE(submission_timestamp) >= '2023-01-01'  -- initialize by looking over all of history
          AND sample_id >= @sample_id
          AND sample_id < @sample_id + @sampling_batch_size
        {% else %}
          DATE(submission_timestamp) = @submission_date
        {% endif %}
      -- remove unnecessary high-volume categories to reduce cost
        AND event_category NOT IN ('media.playback', 'nimbus_events', 'uptake.remotecontent.result')
        AND profile_group_id IS NOT NULL -- only include non-null IDs so as not to create repeats
        -- below is the templated criteria
        AND (
          event_category = 'tabgroup'
          AND (
            (event_name = 'smart_tab_suggest' AND JSON_VALUE(event_extra.action) LIKE 'save%')
            OR (event_name = 'smart_tab_topic' AND JSON_VALUE(event_extra.action) = 'save')
          )
        )
      GROUP BY
        client_id,
        `event`,
        event_category,
        event_name,
        criteria
    )
  UNION ALL
    (
      SELECT
        MIN(submission_timestamp) AS first_submission_timestamp,
        client_id,
        `event`,
        event_category,
        event_name,
        'linkpreview_ai_consent' AS criteria,
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
        `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
      WHERE
        {% if is_init() %}
          DATE(submission_timestamp) >= '2023-01-01'  -- initialize by looking over all of history
          AND sample_id >= @sample_id
          AND sample_id < @sample_id + @sampling_batch_size
        {% else %}
          DATE(submission_timestamp) = @submission_date
        {% endif %}
      -- remove unnecessary high-volume categories to reduce cost
        AND event_category NOT IN ('media.playback', 'nimbus_events', 'uptake.remotecontent.result')
        AND profile_group_id IS NOT NULL -- only include non-null IDs so as not to create repeats
        -- below is the templated criteria
        AND (
          event_category = 'genai.linkpreview'
          AND event_name = 'card_ai_consent'
          AND JSON_VALUE(event_extra.option) = 'continue'
        )
      GROUP BY
        client_id,
        `event`,
        event_category,
        event_name,
        criteria
    )
),
{% if is_init() %}
  final_cte AS (
    SELECT
      *
    FROM
      events_stream_cte
  )
{% else %}
-- query over all of history to see whether the client_id, event and criteria combination has shown up before
  _previous_cte AS (
    SELECT
      client_id,
      `event`,
      criteria,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.events_first_seen_v1`
    WHERE
      DATE(first_submission_timestamp) >= '2023-01-01'
      AND DATE(first_submission_timestamp) < @submission_date
  ),
  final_cte AS (
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
  )
{% endif %}
SELECT
  *
FROM
  final_cte
