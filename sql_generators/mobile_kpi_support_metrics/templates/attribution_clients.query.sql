{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
WITH new_profiles AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
  FROM `{{ project_id }}.{{ dataset }}.active_users`
  WHERE
    submission_date = @submission_date
    AND is_new_profile
)
{% if 'first_session' in product_attribution_group_pings %}
, first_session_ping_base AS (
SELECT
    client_info.client_id,
    sample_id,
    submission_timestamp,
    {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
    {% for field in attribution_group.fields %}
      NULLIF(metrics.string.first_session_{{ field.name.replace('adjust_', '').replace('ad_group', 'adgroup') }}, "") AS {{ field.name }},
    {% endfor %}
    {% endfor %}
    {% for attribution_group in product_attribution_groups if attribution_group.name == 'play_store' %}
    {% for field in attribution_group.fields %}
      {% if field.name == 'play_store_attribution_install_referrer_response' %}
      NULLIF(metrics.text2.{{ field.name }}, "") AS {{ field.name }},
      {% else %}
      NULLIF(metrics.string.{{ field.name }}, "") AS {{ field.name }},
      {% endif %}
    {% endfor %}
    {% endfor %}
    {% for attribution_group in product_attribution_groups if attribution_group.name == 'meta' %}
    {% for field in attribution_group.fields %}
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
    {% if 'adjust' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
        {% for field in attribution_group.fields %}
          {% if not loop.first %}OR {% endif %}{{ field.name }} IS NOT NULL{% if loop.last %},{% endif %}
        {% endfor %}
        {% endfor %}
        STRUCT(
          submission_timestamp,
          {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
          {% for field in attribution_group.fields %}
            {{ field.name }}{% if not loop.last %},{% endif %}
          {% endfor %}
          {% endfor %}
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS adjust_info,
    {% endif %}
    {% if 'play_store' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        {% for attribution_group in product_attribution_groups if attribution_group.name == 'play_store' %}
        {% for field in attribution_group.fields %}
          {% if not loop.first %}OR {% endif %}{{ field.name }} IS NOT NULL{% if loop.last %},{% endif %}
        {% endfor %}
        {% endfor %}
        STRUCT(
          submission_timestamp,
          {% for attribution_group in product_attribution_groups if attribution_group.name == 'play_store' %}
          {% for field in attribution_group.fields %}
            {{ field.name }}{% if not loop.last %},{% endif %}
          {% endfor %}
          {% endfor %}
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS play_store_info,
    {% endif %}
    {% if 'meta' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        {% for attribution_group in product_attribution_groups if attribution_group.name == 'meta' %}
        {% for field in attribution_group.fields %}
          {% if not loop.first %}OR {% endif %}{{ field.name }} IS NOT NULL{% if loop.last %},{% endif %}
        {% endfor %}
        {% endfor %}
        STRUCT(
          submission_timestamp,
          {% for attribution_group in product_attribution_groups if attribution_group.name == 'meta' %}
          {% for field in attribution_group.fields %}
            {{ field.name }}{% if not loop.last %},{% endif %}
          {% endfor %}
          {% endfor %}
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS meta_info,
    {% endif %}
  FROM
    first_session_ping_base
  GROUP BY
    client_id,
    sample_id
)
{% endif %}
{% if 'metrics' in product_attribution_group_pings %}
, metrics_ping_base AS (
  SELECT
    client_info.client_id AS client_id,
    sample_id,
    submission_timestamp,
    {% for attribution_group in product_attribution_groups if attribution_group.name == 'install_source' %}
    {% for field in attribution_group.fields %}
      {% if app_name == 'fenix' %}
      NULLIF(metrics.string.metrics_{{ field.name }}, "") AS {{ field.name }},
      {% else %}
      NULLIF(metrics.string.browser_{{ field.name }}, "") AS {{ field.name }},
      {% endif %}
    {% endfor %}
    {% endfor %}
    {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
    {% for field in attribution_group.fields %}
      NULLIF(metrics.string.metrics_{{ field.name }}, "") AS {{ field.name }},
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
    {% if 'adjust' in product_attribution_group_names %}
    ARRAY_AGG(
      IF(
        {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
        {% for field in attribution_group.fields %}
          {% if not loop.first %}OR {% endif %}{{ field.name }} IS NOT NULL{% if loop.last %},{% endif %}
        {% endfor %}
        {% endfor %}
        STRUCT(
          submission_timestamp,
          {% for attribution_group in product_attribution_groups if attribution_group.name == 'adjust' %}
          {% for field in attribution_group.fields %}
            {{ field.name }}{% if not loop.last %},{% endif %}
          {% endfor %}
          {% endfor %}
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
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
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS install_source,
    {% endif %}
  FROM
    metrics_ping_base
  GROUP BY
    client_id,
    sample_id
)
{% endif %}
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  {% if 'install_source' in product_attribution_group_names %}
  metrics_ping.install_source,
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
FROM
  new_profiles
{% if 'first_session' in product_attribution_group_pings %}LEFT JOIN
  first_session_ping USING(client_id, sample_id)
{% endif %}
{% if 'metrics' in product_attribution_group_pings %}LEFT JOIN
  metrics_ping USING(client_id, sample_id)
{% endif %}
