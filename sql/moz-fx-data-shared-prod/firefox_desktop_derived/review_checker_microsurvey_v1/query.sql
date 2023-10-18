#microsurvey response rates
WITH microsurvey_responses AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    COUNT(DISTINCT CASE WHEN event = 'IMPRESSION' THEN client_id END) AS n_impression,
    COUNT(DISTINCT CASE WHEN event = 'SELECT_CHECKBOX' THEN client_id END) AS n_response,
    REGEXP_EXTRACT(
      message_id,
      'SHOPPING_MICROSURVEY_(\\d)_SHOPPING_MICROSURVEY_SCREEN_.*'
    ) AS question_index
  FROM
    messaging_system.onboarding
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND message_id LIKE '%SHOPPING%'
  GROUP BY
    question_index,
    submission_date
),
filter_ms_microsurvey AS (
  SELECT
    metrics.uuid.messaging_system_client_id AS client_id,
    msg.sample_id,
    msg.normalized_channel AS normalized_channel,
    msg.normalized_country_code AS country_code,
    mozfun.norm.truncate_version(client_info.app_display_version, "major") AS os_version,
    DATE(submission_timestamp) AS submission_date,
    metrics.text.messaging_system_message_id AS message_id,
    metrics.string.messaging_system_event AS event,
    metrics.string.messaging_system_event_page AS event_page,
    metrics.string.messaging_system_event_reason AS event_reason,
    metrics.string.messaging_system_event_source AS event_source,
    IF(DATE_DIFF(DATE(submission_timestamp), first_seen_date, DAY) <= 27, 1, 0) AS new_user,
    IF(DATE_DIFF(DATE(submission_timestamp), first_seen_date, DAY) > 27, 1, 0) AS existing_user,
    CASE
      WHEN metrics.text.messaging_system_message_id = 'SHOPPING_MICROSURVEY_0_SHOPPING_MICROSURVEY_SCREEN_1'
        THEN 'how satisfied'
      WHEN metrics.text.messaging_system_message_id = 'SHOPPING_MICROSURVEY_1_SHOPPING_MICROSURVEY_SCREEN_2'
        THEN 'is useful'
      ELSE NULL
    END AS coded_question,
    CASE
      WHEN metrics.text.messaging_system_message_id = 'SHOPPING_MICROSURVEY_0_SHOPPING_MICROSURVEY_SCREEN_1'
        AND metrics.string.messaging_system_event_source = 'radio-1'
        THEN 'very satisfied'
      WHEN metrics.text.messaging_system_message_id = 'SHOPPING_MICROSURVEY_0_SHOPPING_MICROSURVEY_SCREEN_1'
        AND metrics.string.messaging_system_event_source = 'radio-2'
        THEN 'satisfied'
      WHEN metrics.text.messaging_system_message_id = 'SHOPPING_MICROSURVEY_0_SHOPPING_MICROSURVEY_SCREEN_1'
        AND metrics.string.messaging_system_event_source = 'radio-3'
        THEN 'neutral'
      WHEN metrics.text.messaging_system_message_id = 'SHOPPING_MICROSURVEY_0_SHOPPING_MICROSURVEY_SCREEN_1'
        AND metrics.string.messaging_system_event_source = 'radio-4'
        THEN 'unsatisfied'
      WHEN metrics.text.messaging_system_message_id = 'SHOPPING_MICROSURVEY_0_SHOPPING_MICROSURVEY_SCREEN_1'
        AND metrics.string.messaging_system_event_source = 'radio-5'
        THEN 'very unsatisfied'
      WHEN metrics.text.messaging_system_message_id = 'SHOPPING_MICROSURVEY_1_SHOPPING_MICROSURVEY_SCREEN_2'
        AND metrics.string.messaging_system_event_source = 'radio-1'
        THEN 'yes'
      WHEN metrics.text.messaging_system_message_id = 'SHOPPING_MICROSURVEY_1_SHOPPING_MICROSURVEY_SCREEN_2'
        AND metrics.string.messaging_system_event_source = 'radio-2'
        THEN 'no'
      WHEN metrics.text.messaging_system_message_id = 'SHOPPING_MICROSURVEY_1_SHOPPING_MICROSURVEY_SCREEN_2'
        AND metrics.string.messaging_system_event_source = 'radio-3'
        THEN 'unsure'
      ELSE NULL
    END AS coded_answers,
    ping_info.experiments
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.messaging_system` msg
  LEFT JOIN
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2` cfs
  ON
    cfs.client_id = msg.metrics.uuid.messaging_system_client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.messaging_system_ping_type IS NULL
    AND metrics.text.messaging_system_message_id LIKE '%SHOPPING%'
    AND metrics.string.messaging_system_event = 'SELECT_CHECKBOX'
)
SELECT
  *
FROM
  filter_ms_microsurvey
