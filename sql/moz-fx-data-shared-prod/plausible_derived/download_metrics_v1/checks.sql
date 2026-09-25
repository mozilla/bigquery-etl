#fail
{{ min_row_count(1, "date = @submission_date") }}

#fail
{{ is_unique(["date", "pathname", "entry_page", "country_code", "acquisition_channel", "utm_campaign", "product", "platform", "release_channel", "download_language", "method"], "date = @submission_date") }}

#warn
{{ row_count_within_past_partitions_avg(7, 50, "date") }}
-- Drift warning: flags any props key on product_download events that this
-- query isn't already parsing into its own column, so a new or renamed
-- vendor property gets noticed instead of silently dropped.
--
-- One-off client-side noise is filtered by the
-- record_count threshold below instead of by name, so a
-- similar rare, unnamed noise key in the future gets suppressed
-- automatically rather than needing to be added here.

#warn
WITH unexpected_keys AS (
  SELECT
    -- JSON_KEYS() wraps a key containing "." in double quotes (to
    -- disambiguate a literal dot in the key name from JSONPath's nested-field
    -- syntax, e.g. gtm.uniqueEventId comes back as "gtm.uniqueEventId").
    -- Strip those before comparing so the exclusion list matches, and so a
    -- future dotted/special-character key doesn't slip past this check too.
    TRIM(`key`, '"') AS clean_key,
    COUNT(*) AS record_count
  FROM
    `moz-fx-data-shared-prod.plausible_external.events_v1`,
    UNNEST(JSON_KEYS(SAFE.PARSE_JSON(props))) AS `key`
  WHERE
    DATE(`timestamp`) = @submission_date
    AND name = 'product_download'
    AND TRIM(`key`, '"') NOT IN (
      'product',
      'platform',
      'release_channel',
      'download_language',
      'method',
      'gtm.uniqueEventId'
    )
  GROUP BY
    clean_key
  HAVING
    record_count >= 5
)
SELECT
  IF(
    (SELECT COUNT(*) FROM unexpected_keys) > 0,
    ERROR(
      FORMAT(
        'Unmapped props key(s) found in product_download events on %t -- consider adding a parsed column to plausible_derived.download_metrics_v1: %t',
        @submission_date,
        ARRAY(
          SELECT AS STRUCT
            clean_key AS `key`,
            record_count
          FROM
            unexpected_keys
          ORDER BY
            record_count DESC
        )
      )
    ),
    NULL
  );
