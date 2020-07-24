SELECT
  client_id,
  submission_date,
  COALESCE(protection_report, FALSE) AS protection_report,
  COALESCE(pinned_tab, FALSE) AS pinned_tab,
  COALESCE(default_browser, FALSE) AS default_browser,
  COALESCE(many_uri, FALSE) AS many_uri,
  COALESCE(pip, FALSE) AS pip,
  COALESCE(filled_password, FALSE) AS filled_password,
  COALESCE(remember_password, FALSE) AS remember_password,
  COALESCE(sync_configured, FALSE) AS sync_configured
FROM
  (
    SELECT
      client_id,
      submission_date,
      LOGICAL_OR(event_method = 'show' AND event_object = 'protection_report') AS protection_report,
      LOGICAL_OR(event_category = 'pictureinpicture' AND event_method = 'create') AS pip,
    FROM
      `moz-fx-data-shared-prod.telemetry.events`
    WHERE
      submission_date = @submission_date
    GROUP BY
      submission_date,
      client_id
  ) events_t
FULL JOIN
  (
    SELECT
      client_id,
      DATE(submission_timestamp) AS submission_date,
      SUM(
        payload.processes.parent.scalars.browser_engagement_max_concurrent_tab_pinned_count
      ) > 0 AS pinned_tab,
      LOGICAL_OR(environment.settings.is_default_browser) AS default_browser,
      SUM(payload.processes.parent.scalars.browser_engagement_total_uri_count) > 10 AS many_uri,
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
                IFNULL(
                  mozfun.hist.extract(payload.histograms.pwmgr_form_autofill_result).values,
                  []
                )
              )
            )
        )
      ) AS filled_password,
      LOGICAL_OR(
        1 IN (
          SELECT
            key
          FROM
            UNNEST(
              mozfun.hist.extract(payload.histograms.pwmgr_prompt_remember_action).values
            )
        )
      ) AS remember_password,
    FROM
      `moz-fx-data-shared-prod.telemetry.main`
    WHERE
      DATE(submission_timestamp) = @submission_date
    GROUP BY
      submission_date,
      client_id
  ) main_t
USING
  (client_id, submission_date)
