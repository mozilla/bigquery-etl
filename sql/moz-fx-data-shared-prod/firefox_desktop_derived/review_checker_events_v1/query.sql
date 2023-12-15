SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  MAX(CASE WHEN name = 'surface_onboarding_displayed' THEN 1 ELSE 0 END) is_exposed_event,
  MAX(CASE WHEN name = 'surface_opt_in_clicked' THEN 1 ELSE 0 END) is_opt_in_event,
  MAX(CASE WHEN name = 'surface_displayed' THEN 1 ELSE 0 END) is_surface_displayed,
  MAX(
    CASE
      WHEN name IN (
          'surface_analyze_reviews_none_available_clicked',
          'surface_learn_more_clicked',
          'surface_no_review_reliability_available',
          'surface_not_now_clicked',
          'surface_powered_by_fakespot_link_clicked',
          'surface_reactivated_button_clicked',
          'surface_reanalyze_clicked',
          'surface_settings_expand_clicked',
          'surface_show_more_reviews_button_clicked',
          'surface_show_privacy_policy_clicked',
          'surface_show_quality_explainer_clicked',
          'surface_show_quality_explainer_url_clicked',
          'surface_show_terms_clicked'
        )
        THEN 1
      ELSE 0
    END
  ) is_engaged_with_sidebar,
  ANY_VALUE(ping_info.experiments) AS experiments,
  normalized_channel,
  normalized_country_code,
  sample_id,
  mozfun.norm.truncate_version(client_info.app_display_version, "major") AS os_version,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1` AS e,
  UNNEST(events)
WHERE
  DATE(submission_timestamp) = @submission_date
  AND category = 'shopping'
GROUP BY
  DATE(submission_timestamp),
  client_info.client_id,
  normalized_channel,
  normalized_country_code,
  sample_id,
  os_version
