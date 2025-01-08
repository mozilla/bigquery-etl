WITH first_partition_accounts_backend_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.accounts_backend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_accounts_backend_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.accounts_backend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_accounts_cirrus_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.accounts_cirrus_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_accounts_cirrus_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.accounts_cirrus_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_accounts_frontend_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.accounts_frontend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_accounts_frontend_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.accounts_frontend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_activity_stream_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.activity_stream_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_activity_stream_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.activity_stream_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_ads_backend_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.ads_backend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_ads_backend_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.ads_backend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_bedrock_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_bedrock_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_burnham_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.burnham_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_burnham_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.burnham_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_contextual_services_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.contextual_services_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_contextual_services_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.contextual_services_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_coverage_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.coverage_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_coverage_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.coverage_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_debug_ping_view_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.debug_ping_view_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_debug_ping_view_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.debug_ping_view_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_default_browser_agent_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.default_browser_agent_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_default_browser_agent_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.default_browser_agent_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_eng_workflow_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.eng_workflow_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_eng_workflow_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.eng_workflow_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_firefox_accounts_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_firefox_accounts_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_firefox_crashreporter_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.firefox_crashreporter_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_firefox_crashreporter_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.firefox_crashreporter_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_firefox_desktop_background_defaultagent_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_firefox_desktop_background_defaultagent_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_firefox_desktop_background_tasks_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_firefox_desktop_background_tasks_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_firefox_desktop_background_update_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_update_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_firefox_desktop_background_update_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_update_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_firefox_desktop_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_firefox_desktop_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_firefox_installer_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.firefox_installer_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_firefox_installer_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.firefox_installer_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_firefox_launcher_process_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.firefox_launcher_process_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_firefox_launcher_process_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.firefox_launcher_process_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_firefox_translations_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.firefox_translations_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_firefox_translations_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.firefox_translations_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_glean_dictionary_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.glean_dictionary_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_glean_dictionary_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.glean_dictionary_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_gleanjs_docs_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.gleanjs_docs_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_gleanjs_docs_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.gleanjs_docs_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_mdn_yari_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.mdn_yari_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_mdn_yari_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.mdn_yari_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_messaging_system_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.messaging_system_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_messaging_system_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.messaging_system_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_mobile_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.mobile_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_mobile_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.mobile_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_monitor_backend_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.monitor_backend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_monitor_backend_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.monitor_backend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_monitor_cirrus_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.monitor_cirrus_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_monitor_cirrus_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.monitor_cirrus_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_monitor_frontend_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.monitor_frontend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_monitor_frontend_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.monitor_frontend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_moso_mastodon_backend_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.moso_mastodon_backend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_moso_mastodon_backend_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.moso_mastodon_backend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_moso_mastodon_web_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.moso_mastodon_web_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_moso_mastodon_web_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.moso_mastodon_web_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_mozilla_lockbox_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.mozilla_lockbox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_mozilla_lockbox_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.mozilla_lockbox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_mozilla_mach_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.mozilla_mach_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_mozilla_mach_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.mozilla_mach_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_mozillavpn_backend_cirrus_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_backend_cirrus_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_mozillavpn_backend_cirrus_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_backend_cirrus_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_mozillavpn_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_mozillavpn_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_mozphab_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.mozphab_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_mozphab_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.mozphab_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_net_thunderbird_android_beta_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_beta_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_net_thunderbird_android_beta_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_beta_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_net_thunderbird_android_daily_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_daily_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_net_thunderbird_android_daily_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_daily_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_net_thunderbird_android_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_net_thunderbird_android_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_bergamot_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_bergamot_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_bergamot_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_bergamot_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_connect_firefox_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_connect_firefox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_connect_firefox_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_connect_firefox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_fenix_nightly_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_fenix_nightly_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_fenix_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_fenix_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_fennec_aurora_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_fennec_aurora_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_firefox_beta_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_firefox_beta_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_firefox_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_firefox_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_firefox_vpn_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_firefox_vpn_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_firefoxreality_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefoxreality_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_firefoxreality_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefoxreality_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_focus_beta_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_focus_beta_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_focus_nightly_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_focus_nightly_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_focus_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_focus_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_ios_fennec_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_ios_fennec_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_ios_firefox_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_ios_firefox_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_ios_firefoxbeta_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_ios_firefoxbeta_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_ios_firefoxvpn_network_extension_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_ios_firefoxvpn_network_extension_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_ios_firefoxvpn_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_ios_firefoxvpn_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_ios_focus_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_ios_focus_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_ios_klar_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_klar_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_ios_klar_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_klar_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_ios_lockbox_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_lockbox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_ios_lockbox_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_lockbox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_ios_tiktok_reporter_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_tiktok_reporter_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_ios_tiktok_reporter_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_tiktok_reporter_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_ios_tiktok_reporter_tiktok_reportershare_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_tiktok_reporter_tiktok_reportershare_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_ios_tiktok_reporter_tiktok_reportershare_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_tiktok_reporter_tiktok_reportershare_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_klar_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_klar_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_mozregression_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_mozregression_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_mozregression_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_mozregression_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_reference_browser_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_reference_browser_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_reference_browser_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_reference_browser_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_social_nightly_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_social_nightly_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_social_nightly_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_social_nightly_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_tiktokreporter_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_tiktokreporter_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_tiktokreporter_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_tiktokreporter_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_tv_firefox_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_tv_firefox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_tv_firefox_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_tv_firefox_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_org_mozilla_vrbrowser_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_vrbrowser_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_org_mozilla_vrbrowser_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_vrbrowser_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_pine_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.pine_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_pine_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.pine_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_pocket_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.pocket_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_pocket_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.pocket_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_regrets_reporter_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.regrets_reporter_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_regrets_reporter_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.regrets_reporter_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_regrets_reporter_ucs_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.regrets_reporter_ucs_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_regrets_reporter_ucs_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.regrets_reporter_ucs_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_relay_backend_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.relay_backend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_relay_backend_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.relay_backend_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_syncstorage_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.syncstorage_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_syncstorage_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.syncstorage_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_telemetry_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_telemetry_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_thunderbird_desktop_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.thunderbird_desktop_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_thunderbird_desktop_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.thunderbird_desktop_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_treeherder_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.treeherder_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_treeherder_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.treeherder_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_viu_politica_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.viu_politica_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_viu_politica_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.viu_politica_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
first_partition_webpagetest_stable AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    PARSE_DATE("%Y%m%d", partition_id) AS first_partition_current,
    total_rows AS first_partition_row_count,
  FROM
    `moz-fx-data-shared-prod.webpagetest_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_id) = 1
),
first_non_empty_partition_webpagetest_stable AS (
  SELECT
    table_name,
    PARSE_DATE("%Y%m%d", MIN(partition_id)) AS first_non_empty_partition_current,
  FROM
    `moz-fx-data-shared-prod.webpagetest_stable.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    partition_id != "__NULL__"
    AND total_rows > 0
  GROUP BY
    table_name
),
current_partitions AS (
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_accounts_backend_stable
  LEFT JOIN
    first_non_empty_partition_accounts_backend_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_accounts_cirrus_stable
  LEFT JOIN
    first_non_empty_partition_accounts_cirrus_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_accounts_frontend_stable
  LEFT JOIN
    first_non_empty_partition_accounts_frontend_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_activity_stream_stable
  LEFT JOIN
    first_non_empty_partition_activity_stream_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_ads_backend_stable
  LEFT JOIN
    first_non_empty_partition_ads_backend_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_bedrock_stable
  LEFT JOIN
    first_non_empty_partition_bedrock_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_burnham_stable
  LEFT JOIN
    first_non_empty_partition_burnham_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_contextual_services_stable
  LEFT JOIN
    first_non_empty_partition_contextual_services_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_coverage_stable
  LEFT JOIN
    first_non_empty_partition_coverage_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_debug_ping_view_stable
  LEFT JOIN
    first_non_empty_partition_debug_ping_view_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_default_browser_agent_stable
  LEFT JOIN
    first_non_empty_partition_default_browser_agent_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_eng_workflow_stable
  LEFT JOIN
    first_non_empty_partition_eng_workflow_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_firefox_accounts_stable
  LEFT JOIN
    first_non_empty_partition_firefox_accounts_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_firefox_crashreporter_stable
  LEFT JOIN
    first_non_empty_partition_firefox_crashreporter_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_firefox_desktop_background_defaultagent_stable
  LEFT JOIN
    first_non_empty_partition_firefox_desktop_background_defaultagent_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_firefox_desktop_background_tasks_stable
  LEFT JOIN
    first_non_empty_partition_firefox_desktop_background_tasks_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_firefox_desktop_background_update_stable
  LEFT JOIN
    first_non_empty_partition_firefox_desktop_background_update_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_firefox_desktop_stable
  LEFT JOIN
    first_non_empty_partition_firefox_desktop_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_firefox_installer_stable
  LEFT JOIN
    first_non_empty_partition_firefox_installer_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_firefox_launcher_process_stable
  LEFT JOIN
    first_non_empty_partition_firefox_launcher_process_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_firefox_translations_stable
  LEFT JOIN
    first_non_empty_partition_firefox_translations_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_glean_dictionary_stable
  LEFT JOIN
    first_non_empty_partition_glean_dictionary_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_gleanjs_docs_stable
  LEFT JOIN
    first_non_empty_partition_gleanjs_docs_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_mdn_yari_stable
  LEFT JOIN
    first_non_empty_partition_mdn_yari_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_messaging_system_stable
  LEFT JOIN
    first_non_empty_partition_messaging_system_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_mobile_stable
  LEFT JOIN
    first_non_empty_partition_mobile_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_monitor_backend_stable
  LEFT JOIN
    first_non_empty_partition_monitor_backend_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_monitor_cirrus_stable
  LEFT JOIN
    first_non_empty_partition_monitor_cirrus_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_monitor_frontend_stable
  LEFT JOIN
    first_non_empty_partition_monitor_frontend_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_moso_mastodon_backend_stable
  LEFT JOIN
    first_non_empty_partition_moso_mastodon_backend_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_moso_mastodon_web_stable
  LEFT JOIN
    first_non_empty_partition_moso_mastodon_web_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_mozilla_lockbox_stable
  LEFT JOIN
    first_non_empty_partition_mozilla_lockbox_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_mozilla_mach_stable
  LEFT JOIN
    first_non_empty_partition_mozilla_mach_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_mozillavpn_backend_cirrus_stable
  LEFT JOIN
    first_non_empty_partition_mozillavpn_backend_cirrus_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_mozillavpn_stable
  LEFT JOIN
    first_non_empty_partition_mozillavpn_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_mozphab_stable
  LEFT JOIN
    first_non_empty_partition_mozphab_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_net_thunderbird_android_beta_stable
  LEFT JOIN
    first_non_empty_partition_net_thunderbird_android_beta_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_net_thunderbird_android_daily_stable
  LEFT JOIN
    first_non_empty_partition_net_thunderbird_android_daily_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_net_thunderbird_android_stable
  LEFT JOIN
    first_non_empty_partition_net_thunderbird_android_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_bergamot_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_bergamot_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_connect_firefox_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_connect_firefox_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_fenix_nightly_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_fenix_nightly_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_fenix_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_fenix_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_fennec_aurora_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_fennec_aurora_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_firefox_beta_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_firefox_beta_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_firefox_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_firefox_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_firefox_vpn_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_firefox_vpn_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_firefoxreality_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_firefoxreality_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_focus_beta_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_focus_beta_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_focus_nightly_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_focus_nightly_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_focus_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_focus_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_ios_fennec_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_ios_fennec_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_ios_firefox_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_ios_firefox_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_ios_firefoxbeta_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_ios_firefoxbeta_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_ios_firefoxvpn_network_extension_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_ios_firefoxvpn_network_extension_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_ios_firefoxvpn_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_ios_firefoxvpn_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_ios_focus_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_ios_focus_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_ios_klar_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_ios_klar_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_ios_lockbox_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_ios_lockbox_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_ios_tiktok_reporter_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_ios_tiktok_reporter_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_ios_tiktok_reporter_tiktok_reportershare_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_ios_tiktok_reporter_tiktok_reportershare_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_klar_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_klar_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_mozregression_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_mozregression_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_reference_browser_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_reference_browser_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_social_nightly_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_social_nightly_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_tiktokreporter_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_tiktokreporter_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_tv_firefox_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_tv_firefox_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_org_mozilla_vrbrowser_stable
  LEFT JOIN
    first_non_empty_partition_org_mozilla_vrbrowser_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_pine_stable
  LEFT JOIN
    first_non_empty_partition_pine_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_pocket_stable
  LEFT JOIN
    first_non_empty_partition_pocket_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_regrets_reporter_stable
  LEFT JOIN
    first_non_empty_partition_regrets_reporter_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_regrets_reporter_ucs_stable
  LEFT JOIN
    first_non_empty_partition_regrets_reporter_ucs_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_relay_backend_stable
  LEFT JOIN
    first_non_empty_partition_relay_backend_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_syncstorage_stable
  LEFT JOIN
    first_non_empty_partition_syncstorage_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_telemetry_stable
  LEFT JOIN
    first_non_empty_partition_telemetry_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_thunderbird_desktop_stable
  LEFT JOIN
    first_non_empty_partition_thunderbird_desktop_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_treeherder_stable
  LEFT JOIN
    first_non_empty_partition_treeherder_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_viu_politica_stable
  LEFT JOIN
    first_non_empty_partition_viu_politica_stable
    USING (table_name)
  UNION ALL
  SELECT
    {% if is_init() %}
      CURRENT_DATE() - 1
    {% else %}
      DATE(@submission_date)
    {% endif %} AS run_date,
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    first_partition_current,
    first_non_empty_partition_current,
    first_partition_row_count,
  FROM
    first_partition_webpagetest_stable
  LEFT JOIN
    first_non_empty_partition_webpagetest_stable
    USING (table_name)
),
partition_expirations AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE
    option_name = "partition_expiration_days"
),
partition_stats AS (
  SELECT
    current_partitions.run_date,
    current_partitions.project_id,
    current_partitions.dataset_id,
    current_partitions.table_id,
    COALESCE(
      previous.first_partition_historical,
      current_partitions.first_partition_current
    ) AS first_partition_historical,
    current_partitions.first_partition_current,
    {% if is_init() %}
        -- can't know whether the table had data in the past
        -- if data is already being dropped, so assume it did
      IF(
        DATE_DIFF(
          current_partitions.run_date,
          current_partitions.first_partition_current,
          DAY
        ) >= CAST(option_value AS FLOAT64) - 2,
        current_partitions.first_partition_current,
        current_partitions.first_non_empty_partition_current
      )
    {% else %}
      COALESCE(
        previous.first_non_empty_partition_historical,
        current_partitions.first_non_empty_partition_current
      )
    {% endif %} AS first_non_empty_partition_historical,
    current_partitions.first_non_empty_partition_current,
    current_partitions.first_partition_row_count,
    CAST(CAST(option_value AS FLOAT64) AS INT) AS partition_expiration_days,
    previous.partition_expiration_days AS previous_partition_expiration_days,
  FROM
    current_partitions
  LEFT JOIN
    `moz-fx-data-shared-prod.monitoring_derived.table_partition_expirations_v1` AS previous
    ON current_partitions.project_id = previous.project_id
    AND current_partitions.dataset_id = previous.dataset_id
    AND current_partitions.table_id = previous.table_id
    AND current_partitions.run_date = previous.run_date + 1
  LEFT JOIN
    partition_expirations
    ON table_catalog = current_partitions.project_id
    AND table_schema = current_partitions.dataset_id
    AND table_name = current_partitions.table_id
)
SELECT
  *,
  -- If there was data in the past, then first partition is the next deletion.
  -- Otherwise, next deletion is the first non-empty partition.
  DATE_ADD(
    IF(
      COALESCE(first_non_empty_partition_historical, '9999-12-31') <= first_partition_current,
      first_partition_current,
      first_non_empty_partition_current
    ),
    INTERVAL partition_expiration_days DAY
  ) AS next_deletion_date,
  COALESCE(previous_partition_expiration_days, -1) != COALESCE(
    partition_expiration_days,
    -1
  ) AS expiration_changed,
FROM
  partition_stats
