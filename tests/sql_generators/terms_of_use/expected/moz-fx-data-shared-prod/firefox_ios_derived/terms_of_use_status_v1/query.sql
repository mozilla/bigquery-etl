-- Generated via `terms_of_use` SQL generator.
WITH _previous AS (
  SELECT
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
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.terms_of_use_status_v1`
  WHERE
    DATE(terms_of_use_date_accepted) <> @submission_date
),
_current AS (
  SELECT
    client_info.client_id,
    sample_id,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(app_version_major) OVER _window
    ) AS app_version_major,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(app_version_minor) OVER _window
    ) AS app_version_minor,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(app_version_patch) OVER _window
    ) AS app_version_patch,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(normalized_channel) OVER _window
    ) AS normalized_channel,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(normalized_country_code) OVER _window
    ) AS normalized_country_code,
    `moz-fx-data-shared-prod`.udf.mode_last(ARRAY_AGG(normalized_os) OVER _window) AS normalized_os,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(normalized_os_version) OVER _window
    ) AS normalized_os_version,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(is_bot_generated) OVER _window
    ) AS is_bot_generated,
    `moz-fx-data-shared-prod`.udf.mode_last(ARRAY_AGG(metadata.isp.name) OVER _window) AS isp_name,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(metrics.quantity.user_terms_of_use_version_accepted) OVER _window
    ) AS terms_of_use_version_accepted,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(metrics.datetime.user_terms_of_use_date_accepted) OVER _window
    ) AS terms_of_use_date_accepted,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
    -- Adding a hard filter from when we want to start recording this data:
    AND DATE(submission_timestamp) >= "2025-09-15"
    AND app_version_major >= 142
    AND metrics.datetime.user_terms_of_use_date_accepted IS NOT NULL
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_timestamp DESC) = 1
  WINDOW
    _window AS (
      PARTITION BY
        sample_id,
        client_info.client_id
      ORDER BY
        submission_timestamp
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    )
)
SELECT
  -- update entry if `terms_of_use_version_accepted` or `terms_of_use_date_accepted` value changes:
  IF(
    _previous.client_id IS NULL
    OR (
      (_current.terms_of_use_version_accepted <> _previous.terms_of_use_version_accepted)
      AND (_current.terms_of_use_date_accepted <> _previous.terms_of_use_date_accepted)
    ),
    _current,
    _previous
  ).*
FROM
  _current
FULL OUTER JOIN
  _previous
  USING (client_id, sample_id)
