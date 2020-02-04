WITH nested AS (
  SELECT
    submission_date,
    [
      STRUCT(
        'accessibility' AS tool,
        COUNTIF(scalar_parent_devtools_accessibility_opened_count_sum > 0) AS dau
      ),
      STRUCT('toolbox' AS tool, COUNTIF(devtools_toolbox_opened_count_sum > 0) AS dau),
    -- All histograms below.
      STRUCT(
        'aboutdebugging' AS tool,
        COUNTIF(histogram_parent_devtools_aboutdebugging_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'animationinspector' AS tool,
        COUNTIF(histogram_parent_devtools_animationinspector_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'browserconsole' AS tool,
        COUNTIF(histogram_parent_devtools_browserconsole_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'canvasdebugger' AS tool,
        COUNTIF(histogram_parent_devtools_canvasdebugger_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'computedview' AS tool,
        COUNTIF(histogram_parent_devtools_computedview_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'custom' AS tool,
        COUNTIF(histogram_parent_devtools_custom_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'developertoolbar' AS tool,
        -- no longer available
        -- COUNTIF(histogram_parent_devtools_developertoolbar_opened_count_sum > 0) AS dau
        NULL AS dau
      ),
      STRUCT('dom' AS tool, COUNTIF(histogram_parent_devtools_dom_opened_count_sum > 0) AS dau),
      STRUCT(
        'eyedropper' AS tool,
        COUNTIF(histogram_parent_devtools_eyedropper_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'fontinspector' AS tool,
        COUNTIF(histogram_parent_devtools_fontinspector_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'inspector' AS tool,
        COUNTIF(histogram_parent_devtools_inspector_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'jsbrowserdebugger' AS tool,
        COUNTIF(histogram_parent_devtools_jsbrowserdebugger_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'jsdebugger' AS tool,
        COUNTIF(histogram_parent_devtools_jsdebugger_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'jsprofiler' AS tool,
        COUNTIF(histogram_parent_devtools_jsprofiler_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'layoutview' AS tool,
        COUNTIF(histogram_parent_devtools_layoutview_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'memory' AS tool,
        COUNTIF(histogram_parent_devtools_memory_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'menu_eyedropper' AS tool,
        COUNTIF(histogram_parent_devtools_menu_eyedropper_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'netmonitor' AS tool,
        COUNTIF(histogram_parent_devtools_netmonitor_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'options' AS tool,
        COUNTIF(histogram_parent_devtools_options_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'paintflashing' AS tool,
        COUNTIF(histogram_parent_devtools_paintflashing_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'picker_eyedropper' AS tool,
        COUNTIF(histogram_parent_devtools_picker_eyedropper_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'responsive' AS tool,
        COUNTIF(histogram_parent_devtools_responsive_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'ruleview' AS tool,
        COUNTIF(histogram_parent_devtools_ruleview_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'scratchpad' AS tool,
        COUNTIF(histogram_parent_devtools_scratchpad_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'scratchpad_window' AS tool,
        COUNTIF(histogram_parent_devtools_scratchpad_window_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'shadereditor' AS tool,
        COUNTIF(histogram_parent_devtools_shadereditor_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'storage' AS tool,
        COUNTIF(histogram_parent_devtools_storage_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'styleeditor' AS tool,
        COUNTIF(histogram_parent_devtools_styleeditor_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'webaudioeditor' AS tool,
        COUNTIF(histogram_parent_devtools_webaudioeditor_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'webconsole' AS tool,
        COUNTIF(histogram_parent_devtools_webconsole_opened_count_sum > 0) AS dau
      ),
      STRUCT(
        'webide' AS tool,
        COUNTIF(histogram_parent_devtools_webide_opened_count_sum > 0) AS dau
      )
    ] AS metrics
  FROM
    telemetry.clients_daily
  WHERE
    submission_date = @submission_date
    AND sample_id = 42
    AND devtools_toolbox_opened_count_sum > 0
  GROUP BY
    submission_date
)
SELECT
  submission_date,
  m.*
FROM
  nested
CROSS JOIN
  UNNEST(metrics) AS m
