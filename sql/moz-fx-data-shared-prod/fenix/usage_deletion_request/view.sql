-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.usage_deletion_request`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
  mozfun.norm.fenix_app_info("org_mozilla_firefox", '21850000').channel AS normalized_channel,
  additional_properties,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.usage_deletion_request`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
  mozfun.norm.fenix_app_info("org_mozilla_firefox_beta", '21850000').channel AS normalized_channel,
  additional_properties,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.usage_deletion_request`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
  mozfun.norm.fenix_app_info("org_mozilla_fenix", '21850000').channel AS normalized_channel,
  additional_properties,
  document_id,
  ARRAY(
    SELECT
      STRUCT(
        events.category,
        ARRAY(
          SELECT
            STRUCT(extra.key, extra.value)
          FROM
            UNNEST(events.extra) AS `extra`
        ) AS `extra`,
        events.name,
        events.timestamp
      )
    FROM
      UNNEST(events) AS `events`
  ) AS `events`,
  STRUCT(
    STRUCT(
      metadata.geo.city,
      metadata.geo.country,
      metadata.geo.db_version,
      metadata.geo.subdivision1,
      metadata.geo.subdivision2
    ) AS `geo`,
    STRUCT(
      metadata.header.date,
      metadata.header.dnt,
      metadata.header.x_debug_id,
      metadata.header.x_foxsec_ip_reputation,
      metadata.header.x_lb_tags,
      metadata.header.x_pingsender_version,
      metadata.header.x_source_tags,
      metadata.header.x_telemetry_agent,
      metadata.header.parsed_date,
      metadata.header.parsed_x_source_tags,
      STRUCT(
        metadata.header.parsed_x_lb_tags.tls_version,
        metadata.header.parsed_x_lb_tags.tls_cipher_hex
      ) AS `parsed_x_lb_tags`
    ) AS `header`,
    STRUCT(metadata.isp.db_version, metadata.isp.name, metadata.isp.organization) AS `isp`,
    STRUCT(
      metadata.user_agent.browser,
      metadata.user_agent.os,
      metadata.user_agent.version
    ) AS `user_agent`
  ) AS `metadata`,
  STRUCT(
    STRUCT(
      ARRAY(
        SELECT
          STRUCT(glean_error_invalid_label.key, glean_error_invalid_label.value)
        FROM
          UNNEST(metrics.labeled_counter.glean_error_invalid_label) AS `glean_error_invalid_label`
      ) AS `glean_error_invalid_label`,
      ARRAY(
        SELECT
          STRUCT(glean_error_invalid_overflow.key, glean_error_invalid_overflow.value)
        FROM
          UNNEST(
            metrics.labeled_counter.glean_error_invalid_overflow
          ) AS `glean_error_invalid_overflow`
      ) AS `glean_error_invalid_overflow`,
      ARRAY(
        SELECT
          STRUCT(glean_error_invalid_state.key, glean_error_invalid_state.value)
        FROM
          UNNEST(metrics.labeled_counter.glean_error_invalid_state) AS `glean_error_invalid_state`
      ) AS `glean_error_invalid_state`,
      ARRAY(
        SELECT
          STRUCT(glean_error_invalid_value.key, glean_error_invalid_value.value)
        FROM
          UNNEST(metrics.labeled_counter.glean_error_invalid_value) AS `glean_error_invalid_value`
      ) AS `glean_error_invalid_value`
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.usage_deletion_request`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
  mozfun.norm.fenix_app_info("org_mozilla_fenix_nightly", '21850000').channel AS normalized_channel,
  additional_properties,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.usage_deletion_request`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
  mozfun.norm.fenix_app_info("org_mozilla_fennec_aurora", '21850000').channel AS normalized_channel,
  additional_properties,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.usage_deletion_request`
