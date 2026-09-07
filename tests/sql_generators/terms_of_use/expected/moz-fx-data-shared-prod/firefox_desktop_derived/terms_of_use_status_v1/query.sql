-- Generated via `terms_of_use` SQL generator.
WITH _previous AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    app_version_major,
    app_version_minor,
    app_version_patch,
    normalized_channel,
    normalized_country_code,
    normalized_os,
    normalized_os_version,
    is_bot_generated,
    isp_name,
    terms_of_use_version_accepted,
    terms_of_use_date_accepted,
    legacy_telemetry_client_id,  -- firefox_desktop exclusive field.
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.terms_of_use_status_v1`
  WHERE
    submission_date <> @submission_date
),
_current AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    sample_id,
    mozfun.stats.mode_last(
      ARRAY_AGG(app_version_major ORDER BY submission_timestamp ASC)
    ) AS app_version_major,
    mozfun.stats.mode_last(
      ARRAY_AGG(app_version_minor ORDER BY submission_timestamp ASC)
    ) AS app_version_minor,
    mozfun.stats.mode_last(
      ARRAY_AGG(app_version_patch ORDER BY submission_timestamp ASC)
    ) AS app_version_patch,
    mozfun.stats.mode_last(
      ARRAY_AGG(normalized_channel ORDER BY submission_timestamp ASC)
    ) AS normalized_channel,
    mozfun.stats.mode_last(
      ARRAY_AGG(normalized_country_code ORDER BY submission_timestamp ASC)
    ) AS normalized_country_code,
    mozfun.stats.mode_last(
      ARRAY_AGG(normalized_os ORDER BY submission_timestamp ASC)
    ) AS normalized_os,
    mozfun.stats.mode_last(
      ARRAY_AGG(normalized_os_version ORDER BY submission_timestamp ASC)
    ) AS normalized_os_version,
    mozfun.stats.mode_last(
      ARRAY_AGG(is_bot_generated ORDER BY submission_timestamp ASC)
    ) AS is_bot_generated,
    mozfun.stats.mode_last(
      ARRAY_AGG(metadata.isp.`name` ORDER BY submission_timestamp ASC)
    ) AS isp_name,
    mozfun.stats.mode_last(
      ARRAY_AGG(metrics.quantity.termsofuse_version ORDER BY submission_timestamp ASC)
    ) AS terms_of_use_version_accepted,
    mozfun.stats.mode_last(
      ARRAY_AGG(metrics.datetime.termsofuse_date ORDER BY submission_timestamp ASC)
    ) AS terms_of_use_date_accepted,
    mozfun.stats.mode_last(
      ARRAY_AGG(metrics.uuid.legacy_telemetry_client_id ORDER BY submission_timestamp ASC)
    ) AS legacy_telemetry_client_id,  -- firefox_desktop exclusive field
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
    -- Adding a hard filter from when we want to start recording this data:
    AND DATE(submission_timestamp) >= "2025-06-24"
    AND client_info.client_id IS NOT NULL
    AND app_version_major >= 142
    AND metrics.datetime.termsofuse_date IS NOT NULL
  GROUP BY
    ALL
)
SELECT
  -- update entry if `terms_of_use_version_accepted` or `terms_of_use_date_accepted` value changes:
  IF(
    _previous.client_id IS NULL
    OR (
      (_current.terms_of_use_version_accepted <> _previous.terms_of_use_version_accepted)
      OR (_current.terms_of_use_date_accepted <> _previous.terms_of_use_date_accepted)
    ),
    _current,
    _previous
  ).*,
FROM
  _current
FULL OUTER JOIN
  _previous
  USING (client_id, sample_id)
