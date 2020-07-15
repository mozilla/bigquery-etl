WITH accessibility_panel_client_days AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_channel,
    payload.processes.parent.scalars.devtools_accessibility_opened_count AS opened_count,
    payload.processes.parent.scalars.devtools_accessibility_service_enabled_count AS service_enabled_count,
    payload.processes.parent.scalars.devtools_accessibility_picker_used_count AS picker_used_count,
    payload.processes.parent.scalars.devtools_accessibility_accessible_context_menu_opened AS context_menu_opened_count,
    payload.processes.parent.scalars.devtools_accessibility_node_inspected_count AS node_inspected_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_select_accessible_for_node,
      "browser-context-menu"
    ) AS browser_context_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_select_accessible_for_node,
      "inspector-context-menu"
    ) AS inspector_context_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_audit_activated,
      "ALL"
    ) AS audit_all_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_audit_activated,
      "CONTRAST"
    ) AS audit_contrast_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_audit_activated,
      "KEYBOARD"
    ) AS audit_keyboard_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_audit_activated,
      "TEXT_LABEL"
    ) AS audit_text_label_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_accessible_context_menu_item_activated,
      "print-to-json"
    ) AS print_to_json_context_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
      "accessibility.simulation.deuteranomaly"
    ) AS simulation_deuteranomaly_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
      "accessibility.simulation.protanomaly"
    ) AS simulation_protanomaly_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
      "accessibility.simulation.protanopia"
    ) AS simulation_protanopia_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
      "accessibility.simulation.deuteranopia"
    ) AS simulation_deuteranopia_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
      "accessibility.simulation.tritanopia"
    ) AS simulation_tritanopia_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
      "accessibility.simulation.tritanomaly"
    ) AS simulation_tritanomaly_count,
    udf.get_key(
      payload.processes.parent.keyed_scalars.devtools_accessibility_simulation_activated,
      "accessibility.simulation.contrastLoss"
    ) AS simulation_contrast_loss_count
  FROM
    telemetry.main
  WHERE
    sample_id = 1
    AND DATE(submission_timestamp) = @submission_date
    AND payload.processes.parent.scalars.devtools_accessibility_opened_count > 0
)
SELECT
  submission_date,
  normalized_channel,
  COUNT(*) AS clients,
  SUM(opened_count) AS panel_opens,
  SUM(context_menu_opened_count) / (COUNT(*)) AS context_menu_opened_per_client_day,
  SUM(print_to_json_context_count) / (COUNT(*)) AS print_to_json_context_per_client_day,
  COALESCE(SUM(CAST(print_to_json_context_count > 0 AS INT64)), 0) AS print_to_json_context_users,
  SUM(node_inspected_count) / (COUNT(*)) AS node_inspected_per_client_day,
  COALESCE(SUM(CAST(node_inspected_count > 0 AS INT64)), 0) AS node_inspected_users,
  COALESCE(SUM(CAST(picker_used_count > 0 AS INT64)), 0) AS picker_users,
  COALESCE(SUM(CAST(picker_used_count > 0 AS INT64)), 0) / (
    COUNT(*)
  ) AS picker_users_per_client_day,
  SUM(picker_used_count) / (COUNT(*)) AS picker_count_per_client_day,
  SUM(browser_context_count) / (COUNT(*)) AS browser_context_per_client_day,
  COALESCE(SUM(CAST(browser_context_count > 0 AS INT64)), 0) AS browser_context_users,
  SUM(inspector_context_count) / (COUNT(*)) AS inspector_context_per_client_day,
  COALESCE(SUM(CAST(inspector_context_count > 0 AS INT64)), 0) AS inspector_context_users,
  COALESCE(SUM(CAST(service_enabled_count > 0 AS INT64)), 0) AS service_enabled_users,
  COALESCE(SUM(CAST(service_enabled_count > 0 AS INT64)), 0) / COUNT(
    *
  ) AS service_enabled_users_per_client_day,
  SUM(audit_all_count) / (COUNT(*)) AS audit_all_per_client_day,
  COALESCE(SUM(CAST(audit_all_count > 0 AS INT64)), 0) AS audit_all_users,
  SUM(audit_contrast_count) / (COUNT(*)) AS audit_contrast_per_client_day,
  COALESCE(SUM(CAST(audit_contrast_count > 0 AS INT64)), 0) AS audit_contrast_users,
  SUM(audit_keyboard_count) / (COUNT(*)) AS audit_keyboard_per_client_day,
  COALESCE(SUM(CAST(audit_keyboard_count > 0 AS INT64)), 0) AS audit_keyboard_users,
  SUM(audit_text_label_count) / (COUNT(*)) AS audit_text_label_per_client_day,
  COALESCE(SUM(CAST(audit_text_label_count > 0 AS INT64)), 0) AS audit_text_label_users,
  SUM(simulation_deuteranomaly_count) / (COUNT(*)) AS simulation_deuteranomaly_per_client_day,
  COALESCE(
    SUM(CAST(simulation_deuteranomaly_count > 0 AS INT64)),
    0
  ) AS simulation_deuteranomaly_users,
  SUM(simulation_protanomaly_count) / (COUNT(*)) AS simulation_protanomaly_per_client_day,
  COALESCE(SUM(CAST(simulation_protanomaly_count > 0 AS INT64)), 0) AS simulation_protanomaly_users,
  SUM(simulation_protanopia_count) / (COUNT(*)) AS simulation_protanopia_per_client_day,
  COALESCE(SUM(CAST(simulation_protanopia_count > 0 AS INT64)), 0) AS simulation_protanopia_users,
  SUM(simulation_deuteranopia_count) / (COUNT(*)) AS simulation_deuteranopia_per_client_day,
  COALESCE(
    SUM(CAST(simulation_deuteranopia_count > 0 AS INT64)),
    0
  ) AS simulation_deuteranopia_users,
  SUM(simulation_tritanopia_count) / (COUNT(*)) AS simulation_tritanopia_per_client_day,
  COALESCE(SUM(CAST(simulation_tritanopia_count > 0 AS INT64)), 0) AS simulation_tritanopia_users,
  SUM(simulation_tritanomaly_count) / (COUNT(*)) AS simulation_tritanomaly_per_client_day,
  COALESCE(SUM(CAST(simulation_tritanomaly_count > 0 AS INT64)), 0) AS simulation_tritanomaly_users,
  SUM(simulation_contrast_loss_count) / (COUNT(*)) AS simulation_contrast_loss_per_client_day,
  COALESCE(
    SUM(CAST(simulation_contrast_loss_count > 0 AS INT64)),
    0
  ) AS simulation_contrast_loss_users,
  SUM(
    simulation_deuteranomaly_count + simulation_protanomaly_count + simulation_protanopia_count + simulation_deuteranopia_count + simulation_tritanopia_count + simulation_tritanomaly_count + simulation_contrast_loss_count
  ) AS simulations
FROM
  accessibility_panel_client_days
GROUP BY
  1,
  2
