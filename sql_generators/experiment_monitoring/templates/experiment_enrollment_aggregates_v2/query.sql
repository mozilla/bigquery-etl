-- Generated via ./bqetl generate experiment_monitoring
WITH
{% for app_dataset in applications %}
  {% if "_cirrus" in app_dataset %}
  {{ app_dataset }} AS (
    SELECT
      submission_timestamp AS `timestamp`,
      event.category AS `type`,
      mozfun.map.get_key(event.extra, 'experiment') AS experiment,
      mozfun.map.get_key(event.extra, 'branch') AS branch,
      event.name AS event_method,
      TRUE as validation_errors_valid
    FROM
      `moz-fx-data-shared-prod.{{ app_dataset }}.enrollment`,
      UNNEST(events) AS event
    WHERE
      event.category = 'cirrus_events' AND
      DATE(submission_timestamp) = @submission_date
  ),
  {% else %}
  {{ app_dataset }} AS (
    SELECT
      submission_timestamp AS `timestamp`,
      event_category AS `type`,
      JSON_VALUE(event_extra, '$.experiment') AS experiment,
      JSON_VALUE(event_extra, '$.branch') AS branch,
      event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
      IF(app_version_major >= 109 OR normalized_app_name != 'firefox_desktop', TRUE, FALSE) AS validation_errors_valid
    FROM
      `moz-fx-data-shared-prod.{{ app_dataset }}.events_stream`
    WHERE
      event_category = 'nimbus_events' AND
      DATE(submission_timestamp) = @submission_date
  ),
  {% endif %}
{% endfor %}
all_events AS (
  {% for app_dataset in applications %}
    SELECT
      *
    FROM
      {{ app_dataset }}
    {% if not loop.last %}
      UNION ALL
    {% endif %}
  {% endfor %}
)
SELECT
  `type`,
  experiment,
  branch,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(`timestamp`, HOUR),
    -- Aggregates event counts over 5-minute intervals
    INTERVAL(DIV(EXTRACT(MINUTE FROM `timestamp`), 5) * 5) MINUTE
  ) AS window_start,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(`timestamp`, HOUR),
    INTERVAL((DIV(EXTRACT(MINUTE FROM `timestamp`), 5) + 1) * 5) MINUTE
  ) AS window_end,
  COUNTIF(event_method = 'enroll' OR event_method = 'enrollment') AS enroll_count,
  COUNTIF(event_method = 'unenroll' OR event_method = 'unenrollment') AS unenroll_count,
  COUNTIF(event_method = 'graduate') AS graduate_count,
  COUNTIF(event_method = 'update') AS update_count,
  COUNTIF(event_method = 'enrollFailed') AS enroll_failed_count,
  COUNTIF(event_method = 'unenrollFailed') AS unenroll_failed_count,
  COUNTIF(event_method = 'updateFailed') AS update_failed_count,
  COUNTIF(event_method = 'disqualification') AS disqualification_count,
  COUNTIF(event_method = 'expose' OR event_method = 'exposure') AS exposure_count,
  COUNTIF(event_method = 'validationFailed' AND validation_errors_valid) AS validation_failed_count,
FROM
  all_events
GROUP BY
  `type`,
  experiment,
  branch,
  window_start,
  window_end
