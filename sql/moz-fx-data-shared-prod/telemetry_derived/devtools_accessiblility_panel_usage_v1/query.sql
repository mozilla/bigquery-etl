WITH accessibility_panel_client_days AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    normalized_channel,
    SUM(payload.processes.parent.scalars.devtools_accessibility_opened_count) AS opened_count,
    SUM(
      payload.processes.parent.scalars.devtools_accessibility_service_enabled_count
    ) AS service_enabled_count,
    SUM(
      payload.processes.parent.scalars.devtools_accessibility_picker_used_count
    ) AS picker_used_count,
    SUM(
      payload.processes.parent.scalars.devtools_accessibility_accessible_context_menu_opened
    ) AS context_menu_opened_count,
    SUM(
      payload.processes.parent.scalars.devtools_accessibility_node_inspected_count
    ) AS node_inspected_count,
    SUM(
      payload.processes.parent.scalars.devtools_accessibility_tabbing_order_activated
    ) AS tabbing_order_activated_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_select_accessible_for_node,
        "browser-context-menu"
      )
    ) AS browser_context_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_select_accessible_for_node,
        "inspector-context-menu"
      )
    ) AS inspector_context_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_audit_activated,
        "ALL"
      )
    ) AS audit_all_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_audit_activated,
        "CONTRAST"
      )
    ) AS audit_contrast_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_audit_activated,
        "KEYBOARD"
      )
    ) AS audit_keyboard_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_audit_activated,
        "TEXT_LABEL"
      )
    ) AS audit_text_label_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_accessible_context_menu_item_activated,
        "print-to-json"
      )
    ) AS print_to_json_context_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
        "PROTANOPIA"
      )
    ) AS simulation_protanopia_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
        "DEUTERANOPIA"
      )
    ) AS simulation_deuteranopia_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
        "TRITANOPIA"
      )
    ) AS simulation_tritanopia_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
        "ACHROMATOPSIA"
      )
    ) AS simulation_achromatopsia_count,
    SUM(
      mozfun.map.get_key(
        payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
        "CONTRAST_LOSS"
      )
    ) AS simulation_contrast_loss_count
  FROM
    `moz-fx-data-shared-prod`.telemetry.main
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND payload.processes.parent.scalars.devtools_accessibility_opened_count > 0
  GROUP BY
    submission_date,
    client_id,
    normalized_channel
)
SELECT
  submission_date,
  normalized_channel,
  COUNT(*) AS clients,
  SUM(opened_count) AS panel_opens,
  SAFE_DIVIDE(SUM(opened_count), COUNT(*)) AS panel_opens_per_client_day,
  SAFE_DIVIDE(SUM(context_menu_opened_count), COUNT(*)) AS context_menu_opened_per_client_day,
  SAFE_DIVIDE(SUM(print_to_json_context_count), COUNT(*)) AS print_to_json_context_per_client_day,
  COUNTIF(print_to_json_context_count > 0) AS print_to_json_context_users,
  SAFE_DIVIDE(
    SUM(tabbing_order_activated_count),
    COUNT(*)
  ) AS tabbing_order_activated_per_client_day,
  COUNTIF(tabbing_order_activated_count > 0) AS tabbing_order_activated_users,
  SAFE_DIVIDE(SUM(node_inspected_count), COUNT(*)) AS node_inspected_per_client_day,
  COUNTIF(node_inspected_count > 0) AS node_inspected_users,
  COUNTIF(picker_used_count > 0) AS picker_users,
  COUNTIF(picker_used_count > 0) / (COUNT(*)) AS picker_users_per_client_day,
  SAFE_DIVIDE(SUM(picker_used_count), COUNT(*)) AS picker_count_per_client_day,
  SAFE_DIVIDE(SUM(browser_context_count), COUNT(*)) AS browser_context_per_client_day,
  COUNTIF(browser_context_count > 0) AS browser_context_users,
  SAFE_DIVIDE(SUM(inspector_context_count), COUNT(*)) AS inspector_context_per_client_day,
  COUNTIF(inspector_context_count > 0) AS inspector_context_users,
  COUNTIF(service_enabled_count > 0) AS service_enabled_users,
  SAFE_DIVIDE(COUNTIF(service_enabled_count > 0), COUNT(*)) AS service_enabled_users_per_client_day,
  SAFE_DIVIDE(SUM(audit_all_count), COUNT(*)) AS audit_all_per_client_day,
  COUNTIF(audit_all_count > 0) AS audit_all_users,
  SAFE_DIVIDE(SUM(audit_contrast_count), COUNT(*)) AS audit_contrast_per_client_day,
  COUNTIF(audit_contrast_count > 0) AS audit_contrast_users,
  SAFE_DIVIDE(SUM(audit_keyboard_count), COUNT(*)) AS audit_keyboard_per_client_day,
  COUNTIF(audit_keyboard_count > 0) AS audit_keyboard_users,
  SAFE_DIVIDE(SUM(audit_text_label_count), COUNT(*)) AS audit_text_label_per_client_day,
  COUNTIF(audit_text_label_count > 0) AS audit_text_label_users,
  SAFE_DIVIDE(SUM(simulation_protanopia_count), COUNT(*)) AS simulation_protanopia_per_client_day,
  COUNTIF(simulation_protanopia_count > 0) AS simulation_protanopia_users,
  SAFE_DIVIDE(
    SUM(simulation_deuteranopia_count),
    COUNT(*)
  ) AS simulation_deuteranopia_per_client_day,
  COUNTIF(simulation_deuteranopia_count > 0) AS simulation_deuteranopia_users,
  SAFE_DIVIDE(SUM(simulation_tritanopia_count), COUNT(*)) AS simulation_tritanopia_per_client_day,
  COUNTIF(simulation_tritanopia_count > 0) AS simulation_tritanopia_users,
  SAFE_DIVIDE(
    SUM(simulation_achromatopsia_count),
    COUNT(*)
  ) AS simulation_achromatopsia_per_client_day,
  COUNTIF(simulation_achromatopsia_count > 0) AS simulation_achromatopsia_users,
  SAFE_DIVIDE(
    SUM(simulation_contrast_loss_count),
    COUNT(*)
  ) AS simulation_contrast_loss_per_client_day,
  COUNTIF(simulation_contrast_loss_count > 0) AS simulation_contrast_loss_users,
  SUM(
    simulation_protanopia_count + simulation_deuteranopia_count + simulation_tritanopia_count + simulation_achromatopsia_count + simulation_contrast_loss_count
  ) AS simulations
FROM
  accessibility_panel_client_days
GROUP BY
  submission_date,
  normalized_channel
