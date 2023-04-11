-- Query first observations for Firefox iOS Clients.
WITH first_seen AS (
  SELECT
    client_id,
    submission_date,
    first_run_date,
    first_seen_date,
    sample_id,
    country AS first_reported_country,
    isp AS first_reported_isp,
    normalized_channel AS channel,
    device_manufacturer,
    device_model,
    normalized_os_version AS os_version,
    app_display_version AS app_version
  FROM
    firefox_ios.baseline_clients_first_seen
  WHERE
    submission_date = @submission_date
    AND client_id IS NOT NULL
    -- TODO: is this meant to be restricted to release channel only?
    AND normalized_channel = 'release'
),
-- Find the most recent activation record per client_id.
activations AS (
  SELECT
    client_id,
    ARRAY_AGG(activated ORDER BY submission_date DESC)[SAFE_OFFSET(0)] > 0 AS activated
  FROM
    firefox_ios.new_profile_activation
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
),
-- Find earliest data per client from the first_session ping.
first_session_ping AS (
  SELECT
    client_id,
    MIN(sample_id) AS sample_id,
    DATETIME(MIN(submission_timestamp)) AS min_submission_datetime,
    DATE(MIN(SAFE.PARSE_DATETIME('%F', SUBSTR(first_run_date, 1, 10)))) AS first_run_date,
    ARRAY_AGG(
      IF(
        -- we should probably do the nullif prior to doing this is not null check
        adjust_ad_group IS NOT NULL
        OR adjust_campaign IS NOT NULL
        OR adjust_creative IS NOT NULL
        OR adjust_network IS NOT NULL,
        STRUCT(
          submission_timestamp,
          adjust_ad_group,
          adjust_campaign,
          adjust_creative,
          adjust_network
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        submission_timestamp
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS adjust_info,
  FROM
    (
      SELECT
        client_info.client_id,
        sample_id,
        submission_timestamp,
        client_info.first_run_date,
        NULLIF(metrics.string.adjust_ad_group, "") AS adjust_ad_group,
        NULLIF(metrics.string.adjust_campaign, "") AS adjust_campaign,
        NULLIF(metrics.string.adjust_creative, "") AS adjust_creative,
        NULLIF(metrics.string.adjust_network, "") AS adjust_network,
      FROM
        firefox_ios.first_session
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND client_info.client_id IS NOT NULL
        AND ping_info.seq = 0 -- Pings are sent in sequence, this guarantees that the first one is returned.
    )
  GROUP BY
    client_id
),
-- Find earliest data per client from the metrics ping.
metrics_ping AS (
  SELECT
    client_id,
    MIN(sample_id) AS sample_id,
    DATETIME(
      MIN(submission_timestamp)
    ) AS min_submission_datetime,  -- do we need this? Looks like later we might want to see timestamp corresponding to the row that contains the campaign info
    ARRAY_AGG(
      IF(
        adjust_ad_group IS NOT NULL
        OR adjust_campaign IS NOT NULL
        OR adjust_creative IS NOT NULL
        OR adjust_network IS NOT NULL,
        STRUCT(
          submission_timestamp,
          adjust_ad_group,
          adjust_campaign,
          adjust_creative,
          adjust_network
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        submission_timestamp
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS adjust_info,
  FROM
    (
      SELECT
        client_info.client_id AS client_id,
        sample_id,
        submission_timestamp,
        NULLIF(fxa_metrics.metrics.string.adjust_ad_group, "") AS adjust_ad_group,
        NULLIF(fxa_metrics.metrics.string.adjust_campaign, "") AS adjust_campaign,
        NULLIF(fxa_metrics.metrics.string.adjust_creative, "") AS adjust_creative,
        NULLIF(fxa_metrics.metrics.string.adjust_network, "") AS adjust_network,
        -- NULLIF(metrics.string.metrics_install_source, "") AS install_source,
      FROM
        firefox_ios.metrics AS fxa_metrics
      WHERE
        DATE(submission_timestamp) = @submission_date
        AND client_info.client_id IS NOT NULL
    )
  GROUP BY
    client_id
),
_current AS (
  SELECT
    * EXCEPT (min_submission_datetime, first_run_date, sample_id, adjust_info),
    COALESCE(first_session.adjust_info, metrics.adjust_info) AS adjust_info,
    COALESCE(first_seen.sample_id, first_session.sample_id, metrics.sample_id) AS sample_id,
    activated AS activated,
    STRUCT(
      IF(first_session.client_id IS NULL, FALSE, TRUE) AS reported_first_session_ping,
      IF(metrics.client_id IS NULL, FALSE, TRUE) AS reported_metrics_ping,
      DATE(first_session.min_submission_datetime) AS min_first_session_ping_submission_date,
      DATE(first_session.first_run_date) AS min_first_session_ping_run_date,
      DATE(
        metrics.min_submission_datetime
      ) AS min_metrics_ping_submission_date, -- TODO: are these date fields useful?
      -- # TODO: not quite sure what this case statement is meant to do?
      -- CASE
      --   mozfun.norm.get_earliest_value(
      --     [
      --       (
      --         STRUCT(
      --           CAST(first_session.adjust_network AS STRING),
      --           first_session.min_submission_datetime
      --         )
      --       ),
      --       (STRUCT(CAST(metrics.adjust_network AS STRING), metrics.min_submission_datetime))
      --     ]
      --   )
      --   WHEN STRUCT(first_session.adjust_network, first_session.min_submission_datetime)
      --     THEN 'first_session'
      --   WHEN STRUCT(metrics.adjust_network, metrics.min_submission_datetime)
      --     THEN 'metrics'
      --   ELSE NULL
      -- END AS adjust_network__source_ping,
      -- TODO: is this doing the same as above?
      CASE
        WHEN first_session.adjust_info IS NOT NULL
          THEN "first_session"
        WHEN metrics.adjust_info IS NOT NULL
          THEN "metrics"
        ELSE NULL
      END AS adjust_info__source_ping
    ) AS metadata
  FROM
    first_seen
  FULL OUTER JOIN
    first_session_ping AS first_session
  USING
    (client_id)
  FULL OUTER JOIN
    metrics_ping AS metrics
  USING
    (client_id)
  LEFT JOIN
    activations
  USING
    (client_id)
  WHERE
    client_id IS NOT NULL
),
_previous AS (
  SELECT
    *
  FROM
    firefox_ios_derived.firefox_ios_clients_v1
)
SELECT
  client_id,
  COALESCE(_previous.sample_id, _current.sample_id) AS sample_id,
  COALESCE(_previous.first_seen_date, _current.first_seen_date) AS first_seen_date,
  COALESCE(
    _previous.first_reported_country,
    _current.first_reported_country
  ) AS first_reported_country,
  COALESCE(_previous.first_reported_isp, _current.first_reported_isp) AS first_reported_isp,
  COALESCE(_previous.channel, _current.channel) AS channel,
  COALESCE(_previous.device_manufacturer, _current.device_manufacturer) AS device_manufacturer,
  COALESCE(_previous.device_model, _current.device_model) AS device_model,
  COALESCE(_previous.os_version, _current.os_version) AS os_version,
  COALESCE(_previous.app_version, _current.app_version) AS app_version,
  STRUCT(
    COALESCE(
      _previous.adjust_info.submission_timestamp,
      _current.adjust_info.submission_timestamp
    ) AS submission_timestamp,
    COALESCE(
      _previous.adjust_info.adjust_ad_group,
      _current.adjust_info.adjust_ad_group
    ) AS adjust_ad_group,
    COALESCE(
      _previous.adjust_info.adjust_campaign,
      _current.adjust_info.adjust_campaign
    ) AS adjust_campaign,
    COALESCE(
      _previous.adjust_info.adjust_creative,
      _current.adjust_info.adjust_creative
    ) AS adjust_creative,
    COALESCE(
      _previous.adjust_info.adjust_network,
      _current.adjust_info.adjust_network
    ) AS adjust_network
  ) AS adjust_info,
  STRUCT(
    COALESCE(
      _previous.metadata.reported_first_session_ping,
      _current.metadata.reported_first_session_ping
    ) AS reported_first_session_ping,
    COALESCE(
      _previous.metadata.reported_metrics_ping,
      _current.metadata.reported_metrics_ping
    ) AS reported_metrics_ping,
    COALESCE(
      _previous.metadata.min_first_session_ping_submission_date,
      _current.metadata.min_first_session_ping_submission_date
    ) AS min_first_session_ping_submission_date,
    COALESCE(
      _previous.metadata.min_first_session_ping_run_date,
      _current.metadata.min_first_session_ping_run_date
    ) AS min_first_session_ping_run_date,
    COALESCE(
      _previous.metadata.min_metrics_ping_submission_date,
      _current.metadata.min_metrics_ping_submission_date
    ) AS min_metrics_ping_submission_date,
    COALESCE(
      _previous.metadata.adjust_info__source_ping,
      _current.metadata.adjust_info__source_ping
    ) AS adjust_info__source_ping
  ) AS metadata,
FROM
  _current
FULL OUTER JOIN
  _previous
USING
  (client_id)
