SELECT
  submission_date,
  sample_id,
  client_id,
  COUNT(*) AS n_logged_event,
  COUNTIF(
    event_category = 'pictureinpicture'
    AND event_method = 'create'
  ) AS n_created_pictureinpicture,
  COUNTIF(
    event_category = 'security.ui.protections'
    AND event_object = 'protection_report'
  ) AS n_viewed_protection_report,
  COUNTIF(
    event_category = 'security.ui.protectionspopup'
    AND event_object = 'etp_toggle_off'
  ) AS n_toggled_etp_off,
  COUNTIF(
    event_category = 'security.ui.protectionspopup'
    AND event_object = 'etp_toggle_on'
  ) AS n_toggled_etp_on,
  COUNTIF(
    event_category = 'security.ui.protectionspopup'
    AND event_object = 'protections_popup'
  ) AS n_protections_popup,
  COUNTIF(
    event_category = 'creditcard'
    AND event_object = 'cc_form'
    AND event_method = 'filled'
  ) AS n_ccard_filled,
  COUNTIF(
    event_category = 'creditcard'
    AND event_object = 'capture_doorhanger'
    AND event_method = 'save'
  ) AS n_ccard_saved,
  COUNTIF(
    event_method = 'install'
    AND event_category = 'addonsManager'
    AND event_object = 'extension'
  ) AS n_installed_extension,
  COUNTIF(
    event_method = 'install'
    AND event_category = 'addonsManager'
    AND event_object = 'theme'
  ) AS n_installed_theme,
  COUNTIF(
    event_method = 'install'
    AND event_category = 'addonsManager'
    AND event_object IN ('dictionary', 'locale')
  ) AS n_installed_l10n,
  COUNTIF(event_method = 'saved_login_used') AS n_used_stored_password,
  COUNTIF(
    event_category = 'pwmgr'
    AND event_object IN ('form_login', 'form_password', 'auth_login', 'prompt_login')
  ) AS n_password_filled,
  COUNTIF(
    event_category = 'pwmgr'
    AND event_method = 'doorhanger_submitted'
    AND event_object = 'save'
  ) AS n_password_saved,
  COUNTIF(event_category = 'pwmgr' AND event_method = 'open_management') AS n_pwmgr_opened,
  COUNTIF(
    event_category = 'pwmgr'
    AND event_method IN ('copy', 'show')
  ) AS n_pwmgr_copy_or_show_info,
  COUNTIF(
    event_category = 'pwmgr'
    AND event_method IN ('dismiss_breach_alert', 'learn_more_breach')
  ) AS n_pwmgr_interacted_breach,
  COUNTIF(
    event_object = 'generatedpassword'
    AND event_method = 'autocomplete_field'
  ) AS n_generated_password,
  COUNTIF(event_category = 'activity_stream' AND event_object IN ('CLICK')) AS n_newtab_click,
  COUNTIF(
    event_category = 'activity_stream'
    AND event_object IN ('BOOKMARK_ADD')
  ) AS n_newtab_bookmark_add,
  COUNTIF(
    event_category = 'activity_stream'
    AND event_object IN ('SAVE_TO_POCKET')
  ) AS n_newtab_bookmark_save_to_pocket,
  COUNTIF(
    event_category = 'activity_stream'
    AND event_object IN ('OPEN_NEWTAB_PREFS')
  ) AS n_newtab_open_prefs,
  COUNTIF(event_category = 'fxa' AND event_method = 'connect') AS n_fxa_connect,
  COUNTIF(
    event_category = 'normandy'
    AND event_object IN ("preference_study", "addon_study", "preference_rollout", "addon_rollout")
  ) AS n_normandy_enrolled,
  COUNTIF(event_category = 'messaging_experiments' AND event_method = 'reach') AS n_cfr_qualified,
  COUNTIF(event_category = 'downloads') AS n_downloads,
  COUNTIF(event_category = 'downloads' AND event_string_value = 'pdf') AS n_pdf_downloads,
  COUNTIF(
    event_category = 'downloads'
    AND event_string_value IN ('jpg', 'jpeg', 'png', 'gif')
  ) AS n_image_downloads,
  COUNTIF(
    event_category = 'downloads'
    AND event_string_value IN ('mp4', 'mp3', 'wav', 'mov')
  ) AS n_media_downloads,
  COUNTIF(
    event_category = 'downloads'
    AND event_string_value IN ('xlsx', 'docx', 'pptx', 'xls', 'ppt', 'doc')
  ) AS n_msoffice_downloads,
FROM
  telemetry.events
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  sample_id,
  client_id
