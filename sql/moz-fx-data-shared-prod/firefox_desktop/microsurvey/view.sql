CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.microsurvey`
AS
WITH exclude_event_input_value AS (
  SELECT
    * REPLACE (
      STRUCT(
        metrics.counter,
        metrics.labled_counter,
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
    *
  FROM
    replace_and_lower
)
SELECT
  *
FROM
  final
