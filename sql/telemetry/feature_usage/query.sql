SELECT
  events_t.client_id AS clients_id,
  events_t.submission_date AS submission_date,
  COALESCE(protection_report, 0) AS protection_report,
  COALESCE(pinned_tab, 0) AS pinned_tab,
  COALESCE(default_browser, 0) AS default_browser,
  COALESCE(many_uri, 0) AS many_uri,
  COALESCE(pip, 0) AS pip,
  COALESCE(filled_password, 0) AS filled_password,
  COALESCE(remember_password, 0) AS remember_password,
  COALESCE(sync_configured, 0) AS sync_configured
FROM
  (
    SELECT
      client_id,
      submission_date,
      IF(event_method = 'show' AND event_object = 'protection_report', 1, 0) AS protection_report,
      IF(event_category = 'pictureinpicture' AND event_method = 'create', 1, 0) AS pip,
    FROM
      `moz-fx-data-shared-prod.telemetry.events`
    WHERE
      submission_date = @submission_date
      AND normalized_channel = "release"
  ) events_t
FULL JOIN
  (
    SELECT
      client_id,
      submission_date,
      IF(scalar_parent_browser_engagement_max_concurrent_tab_pinned_count > 0, 1, 0) AS pinned_tab,
    FROM
      `moz-fx-data-shared-prod.telemetry.main_summary`
    WHERE
      submission_date = @submission_date
      AND normalized_channel = "release"
  ) ms_t
ON
  events_t.client_id = ms_t.client_id
  AND events_t.submission_date = ms_t.submission_date
FULL JOIN
  (
    SELECT
      client_id,
      submission_date,
      IF(is_default_browser, 1, 0) AS default_browser,
      IF(scalar_parent_browser_engagement_total_uri_count_sum > 10, 1, 0) AS many_uri,
      IF(sync_count_desktop_sum > 0, 1, 0) AS sync_configured,
    FROM
      `moz-fx-data-shared-prod.telemetry.clients_daily`
    WHERE
      submission_date = @submission_date
      AND normalized_channel = "release"
  ) cd_t
ON
  events_t.client_id = cd_t.client_id
  AND events_t.submission_date = cd_t.submission_date
FULL JOIN
  (
    SELECT
      client_id,
      submission_date,
      1 AS filled_password
    FROM
      (
        (
          SELECT
            client_id,
            submission_date
          FROM
            `moz-fx-data-shared-prod.telemetry.main_summary` ms_t
          CROSS JOIN
            ms_t.histogram_content_pwmgr_form_autofill_result res_a
          WHERE
            submission_date = @submission_date
            AND res_a.key = 0
            AND normalized_channel = "release"
        )
        UNION ALL
          (
            SELECT
              client_id,
              submission_date
            FROM
              `moz-fx-data-shared-prod.telemetry.main_summary` ms_t
            CROSS JOIN
              ms_t.histogram_parent_pwmgr_form_autofill_result res_b
            WHERE
              submission_date = @submission_date
              AND res_b.key = 0
              AND normalized_channel = "release"
          )
      )
  ) filled_password_t
ON
  events_t.client_id = filled_password_t.client_id
  AND events_t.submission_date = filled_password_t.submission_date
FULL JOIN
  (
    SELECT
      client_id,
      submission_date,
      1 AS remember_password
    FROM
      `moz-fx-data-shared-prod.telemetry.main_summary` ms_t
    CROSS JOIN
      ms_t.histogram_parent_pwmgr_prompt_remember_action res_a
    WHERE
      submission_date = @submission_date
      AND res_a.key = 1
      AND normalized_channel = "release"
  ) remember_password_t
ON
  events_t.client_id = remember_password_t.client_id
  AND events_t.submission_date = remember_password_t.submission_date
