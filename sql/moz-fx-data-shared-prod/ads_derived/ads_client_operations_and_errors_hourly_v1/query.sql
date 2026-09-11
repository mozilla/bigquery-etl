-- Hourly aggregate of ads client metrics pings across desktop, iOS and Android platforms.
-- Covers operation counts (labeled_counter) and error occurrence counts (labeled_string)
-- broken down by key, plus total ping count and device dimensions.
-- One `sources` entry per platform/channel combination; desktop ships every channel in a
-- single table, so it only needs one entry despite covering nightly/beta/release.
-- Only pings with at least one ads_client metric entry are included.
--
-- Desktop re-declares the ads_client category in browser/components/newtab/metrics.yaml
-- rather than inheriting the application-services metrics.yaml the mobile apps use (bug
-- 2058567). The two copies have drifted: desktop's http_cache_outcome declares a
-- 'trim_failed' label that mobile's does not, even though the shared Rust component emits
-- it (CacheOutcome::TrimFailed) on every platform. Glean enforces the declared label list,
-- so a trim failure on mobile falls into the '__other__' bucket instead of 'trim_failed'.
-- Each labeled_string group below gets a `*_other` column so a gap like this is visible in
-- the data instead of silently dropped.
{% set sources = [
    {
      'dataset': 'org_mozilla_ios_firefox',
      'surface': 'Mobile',
      'app_name': 'Firefox for iOS',
      'os_expr': 'client_info.os',
      'init_start': '2026-02-01',
    },
    {
      'dataset': 'org_mozilla_firefox',
      'surface': 'Mobile',
      'app_name': 'Fenix',
      'os_expr': 'client_info.os',
      'init_start': '2026-02-01',
    },
    {
      'dataset': 'org_mozilla_fenix',
      'surface': 'Mobile',
      'app_name': 'Fenix',
      'os_expr': 'client_info.os',
      'init_start': '2026-02-01',
    },
    {
      'dataset': 'firefox_desktop',
      'surface': 'Desktop',
      'app_name': 'Firefox Desktop',
      'os_expr': 'normalized_os',
      'init_start': '2026-08-07',
    },
] %}
{% set metric_groups = [
    {
      'column': 'client_operation_total',
      'raw_column': 'metrics.labeled_counter.ads_client_client_operation_total',
      'prefix': 'op',
      'counter': true,
      'other': false,
      'comment': 'Operation totals (labeled_counter: sum values per key)',
      'labels': ['new', 'request_ads', 'record_click', 'record_impression', 'report_ad'],
    },
    {
      'column': 'client_error',
      'raw_column': 'metrics.labeled_string.ads_client_client_error',
      'prefix': 'client_error',
      'counter': false,
      'other': true,
      'comment': 'Client errors (labeled_string: sum total occurrences per key)',
      'labels': ['request_ads', 'record_click', 'record_impression', 'report_ad'],
    },
    {
      'column': 'build_cache_error',
      'raw_column': 'metrics.labeled_string.ads_client_build_cache_error',
      'prefix': 'build_cache_error',
      'counter': false,
      'other': true,
      'comment': 'Build cache errors (labeled_string: sum total occurrences per key)',
      'labels': ['builder_error', 'database_error', 'empty_db_path', 'invalid_max_size', 'invalid_ttl'],
    },
    {
      'column': 'deserialization_error',
      'raw_column': 'metrics.labeled_string.ads_client_deserialization_error',
      'prefix': 'deserialization_error',
      'counter': false,
      'other': true,
      'comment': 'Deserialization errors (labeled_string: sum total occurrences per key)',
      'labels': ['invalid_ad_item', 'invalid_array', 'invalid_structure'],
    },
    {
      'column': 'http_cache_outcome',
      'raw_column': 'metrics.labeled_string.ads_client_http_cache_outcome',
      'prefix': 'http_cache_outcome',
      'counter': false,
      'other': true,
      'comment': 'HTTP cache outcomes (labeled_string: sum total occurrences per key)',
      'labels': ['cleanup_failed', 'hit', 'lookup_failed', 'miss_not_cacheable', 'miss_stored', 'no_cache', 'store_failed', 'trim_failed'],
    },
] %}
WITH base AS (
  {% for source in sources %}
    {% if not loop.first %}
      UNION ALL BY NAME
    {% endif %}
    SELECT
      submission_timestamp,
      normalized_country_code,
      client_info.app_display_version AS app_version,
      normalized_channel,
      '{{ source.surface }}' AS surface,
      {{ source.os_expr }} AS normalized_os,
      '{{ source.app_name }}' AS app_name,
      normalized_channel AS channel,
      {% for group in metric_groups %}
        {{ group.raw_column }} AS {{ group.column }},
      {% endfor %}
    FROM
      {% if is_init() %}
        `moz-fx-data-shared-prod.{{ source.dataset }}_stable.metrics_v1`
      {% else %}
        `moz-fx-data-shared-prod.{{ source.dataset }}_live.metrics_v1`
      {% endif %}
    WHERE
      {% if is_init() %}
        DATE(submission_timestamp) >= DATE("{{ source.init_start }}")
      {% else %}
        DATE(submission_timestamp) = @submission_date
      {% endif %}
      AND (
        {% for group in metric_groups %}
          {% if not loop.first %}
            OR
          {% endif %}
          ARRAY_LENGTH({{ group.raw_column }}) > 0
        {% endfor %}
      )
  {% endfor %}
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
  surface,
  normalized_os,
  app_name,
  channel,
  normalized_country_code,
  app_version,
  normalized_channel,
  COUNT(*) AS ping_count,
  {% for group in metric_groups %}
    -- {{ group.comment }}
    {% for label in group.labels %}
      {% if group.counter %}
        SUM(
          IFNULL((SELECT SUM(value) FROM UNNEST({{ group.column }}) WHERE key = '{{ label }}'), 0)
        ) AS {{ group.prefix ~ '_' ~ label }},
      {% else %}
        SUM(
          (SELECT COUNT(*) FROM UNNEST({{ group.column }}) WHERE key = '{{ label }}')
        ) AS {{ group.prefix ~ '_' ~ label }},
      {% endif %}
    {% endfor %}
    {% if group.other %}
  -- Any label outside the list above (Glean's '__other__' overflow bucket, see header).
      SUM(
        (SELECT COUNT(*) FROM UNNEST({{ group.column }}) WHERE key = '__other__')
      ) AS {{ group.prefix ~ '_other' }},
    {% endif %}
  {% endfor %}
FROM
  base
GROUP BY
  submission_date,
  submission_hour,
  surface,
  normalized_os,
  app_name,
  channel,
  normalized_country_code,
  app_version,
  normalized_channel
