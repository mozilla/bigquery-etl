SELECT
  DATE(main.submission_timestamp) AS submission_date,
  main.normalized_channel AS channel,
  main.environment.system.os.name AS os,
  COUNT(DISTINCT client_id) AS num_clients,
FROM
  `moz-fx-data-shared-prod`.telemetry_stable.main_v5 AS main
CROSS JOIN
  UNNEST(main.payload.processes.parent.keyed_scalars.a11y_theme) AS a11y_theme
WHERE
  {% if is_init() %}
    DATE(main.submission_timestamp) >= '2021-01-01'
  {% else %}
    DATE(main.submission_timestamp) = @submission_date
  {% endif %}
  /*
  a11y_theme.value is TRUE when an OS high contrast mode (HCM) is present and FALSE otherwise,

  a11y_theme.key is derived from the config pref `browser.display.document_color_use" which is has the
  following values / meanings:
  * "always" = always use document colors, regardless of OS HCM or colors chosen in the colors
    dialog (about:preferences > manage colors)
  * "never" = never use document colors (this is equivalent to "always use HCM colors")
  * "default" = use document colors when OS HCM is off, and use OS HCM colors when OS HCM is on.

  This condition selects users who have OS HCM on and who have not forced document colors everywhere
  (ie. the OS HCM colors should propagate to web content).
  */
  AND a11y_theme.value IS TRUE
  AND a11y_theme.key != "always"
GROUP BY
  submission_date,
  channel,
  os
