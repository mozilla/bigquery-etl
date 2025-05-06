{{ header }}
WITH new_profiles AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
    {% if 'distribution_id' in product_attribution_group_names %}
    distribution_id,
    {% endif %}
    {% if 'install_source' in product_attribution_group_names %}
    install_source,
    {% endif %}
    {% if 'is_suspicious_device_client' in product_attribution_group_names %}
    -- field to help us identify suspicious devices on iOS, for more info see: bug-1846554
    (app_display_version = '107.2' AND submission_date >= '2023-02-01') AS is_suspicious_device_client,
    {% endif %}
  FROM `{{ project_id }}.{{ dataset }}.baseline_clients_first_seen`
  WHERE
    submission_date = @submission_date
    AND is_new_profile
)
{% if 'first_session' in product_attribution_group_pings %}
, first_session_ping_base AS (
SELECT
    client_info.client_id,
    sample_id,
    normalized_channel,
    submission_timestamp,
    ping_info.seq AS ping_seq,
    {% if 'distribution_id' in product_attribution_group_names %}
    metrics.string.first_session_distribution_id AS distribution_id,
    {% endif %}
    {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
    {% for field in attribution_group.fields if not field.name.endswith("_timestamp") %}
      {% if app_name == "firefox_ios" %}
        NULLIF(metrics.string.{{ field.name }}, "") AS {{ field.name }},
      {% else %}
        NULLIF(metrics.string.first_session_{{ field.name.replace('adjust_', '').replace('ad_group', 'adgroup') }}, "") AS {{ field.name }},
      {% endif %}
    {% endfor %}
    {% endfor %}
    {% for attribution_group in product_attribution_groups if attribution_group.name == 'play_store' %}
    {% for field in attribution_group.fields if not field.name.endswith("_timestamp") %}
      {% if field.name == 'play_store_attribution_install_referrer_response' %}
      NULLIF(metrics.text2.{{ field.name }}, "") AS {{ field.name }},
      {% else %}
      NULLIF(metrics.string.{{ field.name }}, "") AS {{ field.name }},
      {% endif %}
    {% endfor %}
    {% endfor %}
    {% for attribution_group in product_attribution_groups if attribution_group.name == 'meta' %}
    {% for field in attribution_group.fields if not field.name.endswith("_timestamp") %}
      NULLIF(metrics.string.{{ field.name }}, "") AS {{ field.name }},
    {% endfor %}
    {% endfor %}
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.first_session`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
),
first_session_ping AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    {% if 'adjust' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
        {% for field in attribution_group.fields if not field.name.endswith("_timestamp") %}
          {% if not loop.first %}OR {% endif %}{{ field.name }} IS NOT NULL{% if loop.last %},{% endif %}
        {% endfor %}
        {% endfor %}
        STRUCT(
          {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
          {% for field in attribution_group.fields %}
            {% if field.name.endswith("_timestamp") %}
              submission_timestamp AS {{ field.name }}
            {% else %}
              {{ field.name }}
            {% endif %}
            {% if not loop.last %},{% endif %}
          {% endfor %}
          {% endfor %}
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC, submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS adjust_info,
    {% endif %}
    {% if 'play_store' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        {% for attribution_group in product_attribution_groups if attribution_group.name == 'play_store' %}
        {% for field in attribution_group.fields if not field.name.endswith("_timestamp") %}
          {% if not loop.first %}OR {% endif %}{{ field.name }} IS NOT NULL{% if loop.last %},{% endif %}
        {% endfor %}
        {% endfor %}
        STRUCT(
          {% for attribution_group in product_attribution_groups if attribution_group.name == 'play_store' %}
          {% for field in attribution_group.fields %}
            {% if field.name.endswith("_timestamp") %}
              submission_timestamp AS {{ field.name }}
            {% else %}
              {{ field.name }}
            {% endif %}
            {% if not loop.last %},{% endif %}
          {% endfor %}
          {% endfor %}
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC, submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS play_store_info,
    {% endif %}
    {% if 'meta' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        {% for attribution_group in product_attribution_groups if attribution_group.name == 'meta' %}
        {% for field in attribution_group.fields if not field.name.endswith("_timestamp") %}
          {% if not loop.first %}OR {% endif %}{{ field.name }} IS NOT NULL{% if loop.last %},{% endif %}
        {% endfor %}
        {% endfor %}
        STRUCT(
          {% for attribution_group in product_attribution_groups if attribution_group.name == 'meta' %}
          {% for field in attribution_group.fields %}
            {% if field.name.endswith("_timestamp") %}
              submission_timestamp AS {{ field.name }}
            {% else %}
              {{ field.name }}
            {% endif %}
            {% if not loop.last %},{% endif %}
          {% endfor %}
          {% endfor %}
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC, submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS meta_info,
    {% endif %}
    {% if 'distribution_id' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        distribution_id IS NOT NULL,
        distribution_id,
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC, submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS distribution_id,
    {% endif %}
  FROM
    first_session_ping_base
  GROUP BY
    client_id,
    sample_id,
    normalized_channel
)
{% endif %}
{% if 'metrics' in product_attribution_group_pings %}
, metrics_ping_base AS (
  SELECT
    client_info.client_id AS client_id,
    sample_id,
    normalized_channel,
    submission_timestamp,
    ping_info.seq AS ping_seq,
    {% if 'distribution_id' in product_attribution_group_names %}
    metrics.string.metrics_distribution_id AS distribution_id,
    {% endif %}
    {% for attribution_group in product_attribution_groups if attribution_group.name == 'install_source' %}
    {% for field in attribution_group.fields if not field.name.endswith("_timestamp") %}
      {% if app_name == 'fenix' %}
      NULLIF(metrics.string.metrics_{{ field.name }}, "") AS {{ field.name }},
      {% else %}
      NULLIF(metrics.string.browser_{{ field.name }}, "") AS {{ field.name }},
      {% endif %}
    {% endfor %}
    {% endfor %}
    {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
    {% for field in attribution_group.fields if not field.name.endswith("_timestamp") %}
      {% if app_name == "firefox_ios" %}
      NULLIF(metrics.string.{{ field.name }}, "") AS {{ field.name }},
      {% else %}
      NULLIF(metrics.string.metrics_{{ field.name }}, "") AS {{ field.name }},
      {% endif %}
    {% endfor %}
    {% endfor %}
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.metrics` AS fxa_metrics
  WHERE
    DATE(submission_timestamp)
      BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
      AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
    AND client_info.client_id IS NOT NULL
),
metrics_ping AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    {% if 'adjust' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
        {% for field in attribution_group.fields if not field.name.endswith("_timestamp") %}
          {% if not loop.first %}OR {% endif %}{{ field.name }} IS NOT NULL{% if loop.last %},{% endif %}
        {% endfor %}
        {% endfor %}
        STRUCT(
          {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
          {% for field in attribution_group.fields %}
            {% if field.name.endswith("_timestamp") %}
              submission_timestamp AS {{ field.name }}
            {% else %}
              {{ field.name }}
            {% endif %}
            {% if not loop.last %},{% endif %}
          {% endfor %}
          {% endfor %}
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC, submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS adjust_info,
    {% endif %}
    {% if 'install_source' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        install_source IS NOT NULL,
        install_source,
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC, submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS install_source,
    {% endif %}
    {% if 'distribution_id' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        distribution_id IS NOT NULL,
        distribution_id,
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC, submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS distribution_id,
    {% endif %}
  FROM
    metrics_ping_base
  GROUP BY
    client_id,
    sample_id,
    normalized_channel
)
{% endif %}
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  normalized_channel AS channel,
  {% if 'install_source' in product_attribution_group_names %}
  COALESCE(new_profiles.install_source, metrics_ping.install_source) AS install_source,
  {% endif %}
  {% if 'adjust' in product_attribution_group_names %}
  COALESCE(first_session_ping.adjust_info, metrics_ping.adjust_info) AS adjust_info,
  {% endif %}
  {% if 'play_store' in product_attribution_group_names %}
  first_session_ping.play_store_info,
  {% endif %}
  {% if 'meta' in product_attribution_group_names %}
  first_session_ping.meta_info,
  {% endif %}
  {% if 'is_suspicious_device_client' in product_attribution_group_names %}
  is_suspicious_device_client,
  {% endif %}
  {% if 'distribution_id' in product_attribution_group_names %}
  COALESCE(first_session_ping.distribution_id, new_profiles.distribution_id, metrics_ping.distribution_id) AS distribution_id,
  {% endif %}
FROM
  new_profiles
{% if 'first_session' in product_attribution_group_pings %}LEFT JOIN
  first_session_ping USING(client_id, sample_id, normalized_channel)
{% endif %}
{% if 'metrics' in product_attribution_group_pings %}LEFT JOIN
  metrics_ping USING(client_id, sample_id, normalized_channel)
{% endif %}