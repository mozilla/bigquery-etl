-- Generated via ./bqetl generate glean_usage
-- This table aggregates event collection errors across Glean applications.
-- For details on the error types, see
-- https://mozilla.github.io/glean/book/reference/metrics/event.html#recorded-errors
{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] not in ["telemetry", "accounts_frontend", "accounts_backend"] %}
  {% if not outer_loop.first -%}
  UNION ALL
  {% endif -%}
  (
  WITH event_counters AS (
    SELECT
      DATE(submission_timestamp) AS submission_date,
      client_info.app_channel AS channel,
      metrics.labeled_counter
    FROM
      `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_stable.events_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
  )
  SELECT
    submission_date,
    normalized_app_name,
    channel,
    'overflow' AS error_type,
    KEY AS metric,
    COALESCE(SUM(value), 0) AS error_sum
  FROM
    event_counters,
    UNNEST(labeled_counter.glean_error_invalid_overflow)
  GROUP BY
    submission_date,
    normalized_app_name,
    channel,
    error_type,
    metric
  UNION ALL
  SELECT
    submission_date,
    normalized_app_name,
    channel,
    'invalid_value' AS error_type,
    KEY AS metric,
    COALESCE(SUM(value), 0) AS error_sum
  FROM
    event_counters,
    UNNEST(labeled_counter.glean_error_invalid_value)
  GROUP BY
    submission_date,
    normalized_app_name,
    channel,
    error_type,
    metric
  )
{% elif dataset['bq_dataset_family'] in ["accounts_frontend", "accounts_backend"] %}
  {% if not outer_loop.first -%}
  UNION ALL
  {% endif -%}
  (
  -- FxA events are sent in a custom `accounts_events` ping
  -- Although they do not contain event metrics, this query monitors errors
  -- related to String metrics.
  WITH event_counters AS (
    SELECT
      DATE(submission_timestamp) AS submission_date,
      "{{ dataset['canonical_app_name'] }}" AS normalized_app_name,
      client_info.app_channel AS channel,
      metrics.labeled_counter
    FROM
      `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_stable.accounts_events_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
  )
  SELECT
    submission_date,
    normalized_app_name,
    channel,
    'overflow' AS error_type,
    KEY AS metric,
    COALESCE(SUM(value), 0) AS error_sum
  FROM
    event_counters,
    UNNEST(labeled_counter.glean_error_invalid_overflow)
  GROUP BY
    submission_date,
    normalized_app_name,
    channel,
    error_type,
    metric
  UNION ALL
  SELECT
    submission_date,
    normalized_app_name,
    channel,
    'invalid_value' AS error_type,
    KEY AS metric,
    COALESCE(SUM(value), 0) AS error_sum
  FROM
    event_counters,
    UNNEST(labeled_counter.glean_error_invalid_value)
  GROUP BY
    submission_date,
    normalized_app_name,
    channel,
    error_type,
    metric
  )
{% endif %}
{% endfor %}
{% endfor %}
