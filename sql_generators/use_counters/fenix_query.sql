WITH use_counts_by_day_version_and_country_stg AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    mozfun.norm.truncate_version(client_info.app_display_version, 'major') AS version_major,
    metadata.geo.country AS country,
    'Fenix' AS platform, 
    {% for use_counter_denom in use_counter_denominators %}
      SUM(metrics.counter.{{use_counter_denom.name}}) AS {{use_counter_denom.name}},
    {% endfor %}
    {% for use_counter in use_counters %}
      SUM(metrics.counter.{{use_counter.name}}) AS {{use_counter.name}},
    {% endfor %}
  FROM
    `moz-fx-data-shared-prod.fenix.use_counters` 
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    version_major,
    country,
    platform
  HAVING
    COUNT(DISTINCT(client_info.client_id)) >= 5000
),
pivoted_raw AS (
  SELECT
    *
  FROM
    use_counts_by_day_version_and_country_stg a UNPIVOT(
      cnt FOR metric IN (
        {% for use_counter in use_counters %}
          {{use_counter.name}}

          {% if not loop.last %},
          {% endif %}
        {% endfor %}
      )
    )
),
staging AS (
  SELECT
    submission_date,
    version_major,
    country,
    platform,
    use_counter_content_documents_destroyed,
    use_counter_top_level_content_documents_destroyed,
    use_counter_service_workers_destroyed,
    use_counter_shared_workers_destroyed,
    use_counter_dedicated_workers_destroyed,
    metric,
    cnt,
    CASE
      WHEN metric LIKE 'use_counter_css_doc_%'
        THEN SAFE_DIVIDE(cnt, use_counter_content_documents_destroyed)
      ELSE NULL
    END AS doc_rate,
    CASE
      WHEN metric LIKE 'use_counter_css_page_%'
        THEN SAFE_DIVIDE(cnt, use_counter_top_level_content_documents_destroyed)
      ELSE NULL
    END AS page_rate,
    CASE
      WHEN metric LIKE 'use_counter_worker_service_%'
        THEN SAFE_DIVIDE(cnt, use_counter_service_workers_destroyed)
      ELSE NULL
    END AS service_rate,
    CASE
      WHEN metric LIKE 'use_counter_worker_shared_%'
        THEN SAFE_DIVIDE(cnt, use_counter_shared_workers_destroyed)
      ELSE NULL
    END AS shared_rate,
    CASE
      WHEN metric LIKE 'use_counter_worker_dedicated_%'
        THEN SAFE_DIVIDE(cnt, use_counter_dedicated_workers_destroyed)
      ELSE NULL
    END AS dedicated_rate
  FROM
    pivoted_raw
)
SELECT
  submission_date,
  SAFE_CAST(version_major AS INT64) AS version_major,
  country,
  platform,
  use_counter_content_documents_destroyed,
  use_counter_top_level_content_documents_destroyed,
  use_counter_service_workers_destroyed,
  use_counter_shared_workers_destroyed,
  use_counter_dedicated_workers_destroyed,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(metric, 'use_counter_css_doc_', 'use.counter.css.doc.'),
                  'use_counter_css_page_',
                  'use.counter.css.page.'
                ),
                'use_counter_worker_dedicated',
                'use.counter.worker.dedicated.'
              ),
              'use_counter_worker_shared_',
              'use.counter.worker.shared.'
            ),
            'use_counter_worker_service_',
            'use.counter.worker.service.'
          ),
          'use_counter_deprecated_ops_doc_',
          'use.counter.deprecated_ops.doc.'
        ),
        'use_counter_doc_',
        'use.counter.doc.'
      ),
      'use_counter_page_',
      'use.counter.page.'
    ),
    'use_counter_deprecated_ops_page_',
    'use.counter.deprecated_ops.page.'
  ) AS metric,
  cnt,
  CAST(COALESCE(doc_rate, page_rate, service_rate, shared_rate, dedicated_rate) AS numeric) AS rate
FROM
  staging
