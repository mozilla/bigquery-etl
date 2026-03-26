CREATE MATERIALIZED VIEW
  `moz-fx-data-shared-prod.monitoring_derived.remote_settings_uptake_live_v1`
OPTIONS
  (enable_refresh = TRUE, refresh_interval_minutes = 5)
AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, minute) AS submission_minute,
    SAFE_CAST(mozfun.norm.truncate_version(client_info.app_display_version, 'major') AS INTEGER) AS major_version,
    -- Extra attributes
    -- See https://searchfox.org/firefox-main/rev/1427c88632d1474d/services/common/metrics.yaml
    mozfun.map.get_key(e.extra, 'value') AS extra_value, -- wish it was kept as 'status' when migrated to Glean :(
    mozfun.map.get_key(e.extra, 'trigger') AS extra_trigger,
    mozfun.map.get_key(e.extra, 'source') AS extra_source,
    mozfun.map.get_key(e.extra, 'errorName') AS extra_errorname,
    mozfun.map.get_key(e.extra, 'timestamp') AS extra_timestamp,
    SAFE_CAST(mozfun.map.get_key(e.extra, 'age') AS INT64) AS extra_age,
    SAFE_CAST(mozfun.map.get_key(e.extra, 'duration') AS INT64) AS extra_duration,
    (
      CASE WHEN channel = 'esr' OR channel = 'release'
      THEN
        -- Telemetry is sampled at 1%
        -- See https://experimenter.services.mozilla.com/nimbus/downsample-uptakeremotecontent-events/summary/
        COUNT(DISTINCT client_info.client_id) * 100
      ELSE
        COUNT(DISTINCT client_info.client_id)
      END
    ) AS n_client_id_count,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.events_live`
  INNER JOIN UNNEST(events) AS e ON
    e.category = 'uptake.remotecontent.result'
    AND e.name = 'uptake_remotesettings'
  GROUP BY
    1
)
