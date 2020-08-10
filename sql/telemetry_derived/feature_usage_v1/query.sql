WITH events_t AS (
  SELECT
    submission_date,
    sample_id,
    client_id,
    LOGICAL_OR(
      event_method = 'show'
      AND event_object = 'protection_report'
    ) AS viewed_protection_report,
    LOGICAL_OR(event_category = 'pictureinpicture' AND event_method = 'create') AS used_pip,
    LOGICAL_OR(event_string_value = 'SEC_ERROR_UNKNOWN_ISSUER') AS had_cert_error,
  FROM
    `moz-fx-data-shared-prod.telemetry.events`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    sample_id,
    client_id
),
main_t AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    sample_id,
    client_id,
    SUM(
      payload.processes.parent.scalars.browser_engagement_max_concurrent_tab_pinned_count
    ) > 0 AS has_pinned_tab,
    SUM(ARRAY_LENGTH(environment.addons.active_addons)) > 0 AS has_addon,
    LOGICAL_OR(environment.settings.is_default_browser) AS default_browser,
    SUM(
      payload.processes.parent.scalars.browser_engagement_total_uri_count
    ) >= 10 AS visited_10_uri,
    SUM(
      `moz-fx-data-shared-prod.udf.histogram_max_key_with_nonzero_value`(
        payload.histograms.weave_device_count_desktop
      )
    ) > 0 AS sync_configured,
    LOGICAL_OR(
      0 IN (
        SELECT
          key
        FROM
          UNNEST(
            ARRAY_CONCAT(
              IFNULL(
                mozfun.hist.extract(
                  payload.processes.content.histograms.pwmgr_form_autofill_result
                ).values,
                []
              ),
              IFNULL(mozfun.hist.extract(payload.histograms.pwmgr_form_autofill_result).values, [])
            )
          )
        WHERE
          value > 0
      )
    ) AS filled_password_automatically,
    LOGICAL_OR(
      1 IN (
        SELECT
          key
        FROM
          UNNEST(mozfun.hist.extract(payload.histograms.pwmgr_prompt_remember_action).values)
        WHERE
          value > 0
      )
    ) AS remembered_password,
  FROM
    `moz-fx-data-shared-prod.telemetry.main`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    sample_id,
    client_id
)
SELECT
  submission_date,
  sample_id,
  client_id,
  COALESCE(viewed_protection_report, FALSE) AS viewed_protection_report,
  COALESCE(has_pinned_tab, FALSE) AS has_pinned_tab,
  COALESCE(default_browser, FALSE) AS default_browser,
  COALESCE(visited_10_uri, FALSE) AS visited_10_uri,
  COALESCE(used_pip, FALSE) AS used_pip,
  COALESCE(filled_password_automatically, FALSE) AS filled_password_automatically,
  COALESCE(remembered_password, FALSE) AS remembered_password,
  COALESCE(sync_configured, FALSE) AS sync_configured,
  COALESCE(had_cert_error, FALSE) AS had_cert_error,
  COALESCE(has_addon, FALSE) AS has_addon
FROM
  events_t
FULL JOIN
  main_t
USING
  (submission_date, sample_id, client_id)
