-- Generated via bigquery_etl.glean_usage
{% if is_init() %}
  (
    WITH eventsstream AS (
      SELECT
        MIN(submission_timestamp) AS first_submission_timestamp,
        client_id,
        `event`,
        event_category,
        event_name,
        CAST(NULL AS string) AS criteria,
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
            COALESCE(event_timestamp, '9999-12-31 23:59:59')
          LIMIT
            1
        )[0].*
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
      WHERE
        -- initialize by looking over all of history
        DATE(submission_timestamp) >= '2023-01-01'
        AND sample_id >= @sample_id
        AND sample_id < @sample_id + @sampling_batch_size
        AND event_category NOT IN ('media.playback', 'nimbus_events', 'uptake.remotecontent.result')
        -- if app_id is firefox_desktop, filter for where profile_group_id is not null
        AND profile_group_id IS NOT NULL
        -- below is the templated criteria
        AND (TRUE)
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
  UNION ALL
    (
      WITH eventsstream AS (
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
              COALESCE(event_timestamp, '9999-12-31 23:59:59')
            LIMIT
              1
          )[0].*
        FROM
          `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
        WHERE
        -- initialize by looking over all of history
          DATE(submission_timestamp) >= '2023-01-01'
          AND sample_id >= @sample_id
          AND sample_id < @sample_id + @sampling_batch_size
          AND event_category NOT IN (
            'media.playback',
            'nimbus_events',
            'uptake.remotecontent.result'
          )
        -- if app_id is firefox_desktop, filter for where profile_group_id is not null
          AND profile_group_id IS NOT NULL
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
      SELECT
        *
      FROM
        eventsstream
    )
  UNION ALL
    (
      WITH eventsstream AS (
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
              COALESCE(event_timestamp, '9999-12-31 23:59:59')
            LIMIT
              1
          )[0].*
        FROM
          `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
        WHERE
        -- initialize by looking over all of history
          DATE(submission_timestamp) >= '2023-01-01'
          AND sample_id >= @sample_id
          AND sample_id < @sample_id + @sampling_batch_size
          AND event_category NOT IN (
            'media.playback',
            'nimbus_events',
            'uptake.remotecontent.result'
          )
        -- if app_id is firefox_desktop, filter for where profile_group_id is not null
          AND profile_group_id IS NOT NULL
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
      SELECT
        *
      FROM
        eventsstream
    )
  UNION ALL
    (
      WITH eventsstream AS (
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
              COALESCE(event_timestamp, '9999-12-31 23:59:59')
            LIMIT
              1
          )[0].*
        FROM
          `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
        WHERE
        -- initialize by looking over all of history
          DATE(submission_timestamp) >= '2023-01-01'
          AND sample_id >= @sample_id
          AND sample_id < @sample_id + @sampling_batch_size
          AND event_category NOT IN (
            'media.playback',
            'nimbus_events',
            'uptake.remotecontent.result'
          )
        -- if app_id is firefox_desktop, filter for where profile_group_id is not null
          AND profile_group_id IS NOT NULL
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
      SELECT
        *
      FROM
        eventsstream
    )
{% else %}
  (
    WITH _current AS (
      SELECT
        MIN(submission_timestamp) AS first_submission_timestamp,
        client_id,
        `event`,
        event_category,
        event_name,
        CAST(NULL AS string) AS criteria,
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
            COALESCE(event_timestamp, '9999-12-31 23:59:59')
          LIMIT
            1
        )[0].*
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND event_category NOT IN ('media.playback', 'nimbus_events', 'uptake.remotecontent.result')
        -- if app_id is firefox_desktop, filter for where profile_group_id is not null
        AND profile_group_id IS NOT NULL
        -- below is the templated criteria
        AND (TRUE)
      GROUP BY
        client_id,
        `event`,
        event_category,
        event_name,
        criteria
    ),
  -- query over all of history to see whether the client_id, event and criteria combination has shown up before
    _previous AS (
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
        `moz-fx-data-shared-prod.firefox_desktop_derived.events_first_seen_v1`
      WHERE
        DATE(first_submission_timestamp) >= '2023-01-01'
        AND DATE(first_submission_timestamp) < @submission_date
        AND criteria IS NOT DISTINCT FROM CAST(NULL AS string)
    ),
    _joined AS (
    --switch to using separate if statements instead of 1
    --because dry run is struggling to validate the final struct
      SELECT
        IF(_previous.client_id IS NULL, _current, _previous).*
      FROM
        _current
      FULL OUTER JOIN
        _previous
        ON _current.client_id = _previous.client_id
        AND _current.event = _previous.event
        AND (
          _current.criteria = _previous.criteria
          OR (_current.criteria IS NULL AND _previous.criteria IS NULL)
        )
    )
    SELECT
      *
    FROM
      _joined
  )
  UNION ALL
    (
      WITH _current AS (
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
              COALESCE(event_timestamp, '9999-12-31 23:59:59')
            LIMIT
              1
          )[0].*
        FROM
          `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
        WHERE
          DATE(submission_timestamp) = @submission_date
          AND event_category NOT IN (
            'media.playback',
            'nimbus_events',
            'uptake.remotecontent.result'
          )
        -- if app_id is firefox_desktop, filter for where profile_group_id is not null
          AND profile_group_id IS NOT NULL
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
      ),
  -- query over all of history to see whether the client_id, event and criteria combination has shown up before
      _previous AS (
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
          `moz-fx-data-shared-prod.firefox_desktop_derived.events_first_seen_v1`
        WHERE
          DATE(first_submission_timestamp) >= '2023-01-01'
          AND DATE(first_submission_timestamp) < @submission_date
          AND criteria IS NOT DISTINCT FROM 'chatbot_usage'
      ),
      _joined AS (
    --switch to using separate if statements instead of 1
    --because dry run is struggling to validate the final struct
        SELECT
          IF(_previous.client_id IS NULL, _current, _previous).*
        FROM
          _current
        FULL OUTER JOIN
          _previous
          ON _current.client_id = _previous.client_id
          AND _current.event = _previous.event
          AND (
            _current.criteria = _previous.criteria
            OR (_current.criteria IS NULL AND _previous.criteria IS NULL)
          )
      )
      SELECT
        *
      FROM
        _joined
    )
  UNION ALL
    (
      WITH _current AS (
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
              COALESCE(event_timestamp, '9999-12-31 23:59:59')
            LIMIT
              1
          )[0].*
        FROM
          `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
        WHERE
          DATE(submission_timestamp) = @submission_date
          AND event_category NOT IN (
            'media.playback',
            'nimbus_events',
            'uptake.remotecontent.result'
          )
        -- if app_id is firefox_desktop, filter for where profile_group_id is not null
          AND profile_group_id IS NOT NULL
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
      ),
  -- query over all of history to see whether the client_id, event and criteria combination has shown up before
      _previous AS (
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
          `moz-fx-data-shared-prod.firefox_desktop_derived.events_first_seen_v1`
        WHERE
          DATE(first_submission_timestamp) >= '2023-01-01'
          AND DATE(first_submission_timestamp) < @submission_date
          AND criteria IS NOT DISTINCT FROM 'smart_tabgroup_save'
      ),
      _joined AS (
    --switch to using separate if statements instead of 1
    --because dry run is struggling to validate the final struct
        SELECT
          IF(_previous.client_id IS NULL, _current, _previous).*
        FROM
          _current
        FULL OUTER JOIN
          _previous
          ON _current.client_id = _previous.client_id
          AND _current.event = _previous.event
          AND (
            _current.criteria = _previous.criteria
            OR (_current.criteria IS NULL AND _previous.criteria IS NULL)
          )
      )
      SELECT
        *
      FROM
        _joined
    )
  UNION ALL
    (
      WITH _current AS (
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
              COALESCE(event_timestamp, '9999-12-31 23:59:59')
            LIMIT
              1
          )[0].*
        FROM
          `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
        WHERE
          DATE(submission_timestamp) = @submission_date
          AND event_category NOT IN (
            'media.playback',
            'nimbus_events',
            'uptake.remotecontent.result'
          )
        -- if app_id is firefox_desktop, filter for where profile_group_id is not null
          AND profile_group_id IS NOT NULL
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
      ),
  -- query over all of history to see whether the client_id, event and criteria combination has shown up before
      _previous AS (
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
          `moz-fx-data-shared-prod.firefox_desktop_derived.events_first_seen_v1`
        WHERE
          DATE(first_submission_timestamp) >= '2023-01-01'
          AND DATE(first_submission_timestamp) < @submission_date
          AND criteria IS NOT DISTINCT FROM 'linkpreview_ai_consent'
      ),
      _joined AS (
    --switch to using separate if statements instead of 1
    --because dry run is struggling to validate the final struct
        SELECT
          IF(_previous.client_id IS NULL, _current, _previous).*
        FROM
          _current
        FULL OUTER JOIN
          _previous
          ON _current.client_id = _previous.client_id
          AND _current.event = _previous.event
          AND (
            _current.criteria = _previous.criteria
            OR (_current.criteria IS NULL AND _previous.criteria IS NULL)
          )
      )
      SELECT
        *
      FROM
        _joined
    )
{% endif %}
