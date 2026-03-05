CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.microsurvey_redacted`
AS
WITH exclude_event_input_value AS (
  SELECT
    * REPLACE (
      STRUCT(
        metrics.counter,
        metrics.labeled_counter,
        metrics.quantity,
        metrics.string,
        metrics.string_list,
        STRUCT(
          metrics.text2.microsurvey_event_context,
          metrics.text2.microsurvey_event_screen_family,
          metrics.text2.microsurvey_event_screen_id,
          metrics.text2.microsurvey_event_screen_initials,
          metrics.text2.microsurvey_message_id
        ) AS text2,
        metrics.uuid
      ) AS metrics
    )
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.microsurvey_v1`
),
replace_and_lower AS (
  SELECT
    * REPLACE (mozfun.norm.metadata(metadata) AS metadata),
    LOWER(IFNULL(metadata.isp.name, "")) = "browserstack" AS is_bot_generated,
  FROM
    exclude_event_input_value
),
final AS (
  SELECT
    additional_properties,
    document_id,
    events.category,
    events.extra.key,
    events.extra.value,
    events.name,
    events.timestamp,
    metadata.geo.city,
    metadata.geo.country,
    metadata.geo.db_version,
    metadata.geo.subdivision1,
    metadata.geo.subdivision2,
    metadata.header.date,
    metadata.header.dnt,
    metadata.header.x_debug_id,
    metadata.header.x_foxsec_ip_reputation,
    metadata.header.x_lb_tags,
    metadata.header.x_pingsender_version,
    metadata.header.x_source_tags,
    metadata.header.x_telemetry_agent,
    metadata.isp.db_version,
    metadata.isp.name,
    metadata.isp.organization,
    metadata.user_agent.browser,
    metadata.user_agent.os,
    metadata.user_agent.version,
    metrics,
    normalized_app_name,
    normalized_channel,
    normalized_country_code,
    normalized_os,
    normalized_os_version,
    sample_id,
    subimssion_timestamp,
    is_bot_generated
  FROM
    replace_and_lower
)
SELECT
  *
FROM
  final
