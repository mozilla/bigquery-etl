-- The single source of truth for experiment-specific engagement rows. Each entry adds
-- '<region>-<slug>-<branch>' rows to the artifact for every branch the ping reports, and
-- every branch named in hour_adjusted_branches carries position-AND-hour-adjusted exposure
-- instead of the position-only exposure each other row carries.
--
-- Both the experiment_configs CTE and the hour-adjusted region list below are rendered from
-- this list, so a slug cannot be rotated in one and forgotten in the other. Leave
-- hour_adjusted_branches empty to emit an experiment's rows without any hour adjustment.
--
-- ctrpred_engb is the live A/B for the time-of-day weight: its treatment arm ranks on
-- doubly-adjusted exposure while its control arm keeps vanilla position-only exposure, on
-- identical traffic. Scoping to a region-experiment instead of changing impression_count
-- everywhere keeps the blast radius off every other consumer -- the Thompson prior's
-- concentration, the LIMIT 25000 row cap's ordering and Merino's freshness thresholds all keep
-- their current meaning for the global and per-country rows.
--
-- newtab_merino_priors_v1 keeps its own copy of the region/slug pairs; update both together.
{% set experiments = [
     {'region': 'GB', 'slug': 'ctrpred_engb', 'hour_adjusted_branches': ['treatment']},
   ] %}
{% set hour_propensity_regions = [] %}
{% for experiment in experiments %}
  {% for branch in experiment.hour_adjusted_branches %}
    {% set _ = hour_propensity_regions.append(
         experiment.region ~ '-' ~ experiment.slug ~ '-' ~ branch
       ) %}
  {% endfor %}
{% endfor %}
WITH experiment_configs AS (
  SELECT
    *
  FROM
    UNNEST(
      [
        {% for experiment in experiments %}
          STRUCT('{{ experiment.region }}' AS region, '{{ experiment.slug }}' AS experiment_slug)
          {% if not loop.last %},
          {% endif %}
        {% endfor %}
      ]
    )
),
private_pings AS (
  SELECT
    p.submission_timestamp,
    p.document_id,
    p.events,
    p.normalized_country_code,
    ec.experiment_slug,
    IF(ec.experiment_slug IS NOT NULL, p.experiment_branch, NULL) AS experiment_branch
  FROM
    (
      SELECT
        submission_timestamp,
        document_id,
        events,
        mozfun.newtab.surface_id_country(
          metrics.string.newtab_content_surface_id,
          NULL,
          metrics.string.newtab_content_country
        ) AS normalized_country_code,
        NULLIF(metrics.string.newtab_content_experiment_name, '') AS experiment_slug,
        NULLIF(metrics.string.newtab_content_experiment_branch, '') AS experiment_branch
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_live.newtab_content_v1`
      WHERE
        submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    ) p
  LEFT JOIN
    experiment_configs ec
    ON ec.region = p.normalized_country_code
    AND ec.experiment_slug = p.experiment_slug
),
deduplicated_pings AS (
  SELECT
    *
  FROM
    private_pings
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        DATE(submission_timestamp),
        document_id
      ORDER BY
        submission_timestamp DESC
    ) = 1
),
flattened_newtab_events AS (
  SELECT
    document_id,
    submission_timestamp,
    {% if hour_propensity_regions %}
      -- Only the in-scope experiment branches need an hour grain. Every other row gets a
      -- single NULL hour so raw_grouped_totals keeps its current cardinality, instead of
      -- fanning out by up to 24x through the four position-weight joins below -- a cost
      -- this query would pay on every run, several times an hour, for rows that never
      -- read hour_adjusted_impression_count.
      IF(
        CONCAT(normalized_country_code, '-', experiment_slug, '-', experiment_branch) IN UNNEST(
          {{ hour_propensity_regions }}
        ),
        EXTRACT(HOUR FROM submission_timestamp),
        NULL
      ) AS event_hour,
    {% endif %}
    normalized_country_code,
    experiment_slug,
    experiment_branch,
    unnested_events.name AS event_name,
    mozfun.map.get_key(unnested_events.extra, 'corpus_item_id') AS corpus_item_id,
    SAFE_CAST(mozfun.map.get_key(unnested_events.extra, 'position') AS INT64) AS position,
    mozfun.map.get_key(unnested_events.extra, 'format') AS format,
    SAFE_CAST(
      mozfun.map.get_key(unnested_events.extra, 'section_position') AS INT64
    ) AS section_position
  FROM
    deduplicated_pings dp
  CROSS JOIN
    UNNEST(dp.events) AS unnested_events
  WHERE
    -- Filter to relevant events only
    unnested_events.category IN ('pocket', 'newtab_content')
    AND unnested_events.name IN ('impression', 'click', 'report_content_submit')
    -- Keep only rows with a non-null corpus_item_id
    AND mozfun.map.get_key(unnested_events.extra, 'corpus_item_id') IS NOT NULL
),
raw_grouped_totals AS (
  SELECT
    normalized_country_code,
    experiment_slug,
    experiment_branch,
    corpus_item_id,
    position,
    format,
    section_position,
    {% if hour_propensity_regions %}
      event_hour,
    {% endif %}
    SUM(CASE WHEN event_name = 'impression' THEN 1 ELSE 0 END) AS raw_impression_count,
    SUM(CASE WHEN event_name = 'click' THEN 1 ELSE 0 END) AS click_count,
    SUM(CASE WHEN event_name = 'report_content_submit' THEN 1 ELSE 0 END) AS report_count
  FROM
    flattened_newtab_events
  GROUP BY
    normalized_country_code,
    experiment_slug,
    experiment_branch,
    corpus_item_id,
    position,
    format,
    section_position
    {% if hour_propensity_regions %},
      event_hour
    {% endif %}
),
{% if hour_propensity_regions %}
  hour_propensity_weights AS (
    SELECT
      country,
      `hour`,
      weight
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.newtab_merino_hour_propensity_v1`
    WHERE
      snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
      -- The global (country IS NULL) rows describe all-country pooled exposure and are
      -- deliberately not joinable here.
      AND country IS NOT NULL
    QUALIFY
      snapshot_date = MAX(snapshot_date) OVER ()
  ),
{% endif %}
propensity_weights AS (
  SELECT
    country,
    position,
    tile_format,
    weight
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.newtab_merino_propensity_v2`
  WHERE
    layout = 'SECTION_GRID'
    AND section_position IS NULL
    AND snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
  QUALIFY
    snapshot_date = MAX(snapshot_date) OVER ()
),
/* Separate and adjust section events */
section_events AS (
  SELECT
    rw.normalized_country_code,
    rw.experiment_slug,
    rw.experiment_branch,
    rw.corpus_item_id,
    rw.raw_impression_count,
    -- apply propensity scaling to impressions only
    -- prefer exact-format weights, then fall back to 'any' format weights
    rw.raw_impression_count / COALESCE(
      wt_country_exact.weight,
      wt_global_exact.weight,
      wt_country_any.weight,
      wt_global_any.weight,
      1.0
    ) AS adjusted_impression_count,
    rw.report_count,
    rw.click_count
    {% if hour_propensity_regions %},
      rw.event_hour
    {% endif %}
  FROM
    raw_grouped_totals rw
  LEFT JOIN
    propensity_weights wt_country_exact
    ON wt_country_exact.country = rw.normalized_country_code
    AND SAFE_CAST(wt_country_exact.position AS INT64) = rw.position
    AND wt_country_exact.tile_format = rw.format
  LEFT JOIN
    propensity_weights wt_country_any
    ON wt_country_any.country = rw.normalized_country_code
    AND SAFE_CAST(wt_country_any.position AS INT64) = rw.position
    AND wt_country_any.tile_format = 'any'
  LEFT JOIN
    propensity_weights wt_global_exact
    ON wt_global_exact.country IS NULL
    AND SAFE_CAST(wt_global_exact.position AS INT64) = rw.position
    AND wt_global_exact.tile_format = rw.format
  LEFT JOIN
    propensity_weights wt_global_any
    ON wt_global_any.country IS NULL
    AND SAFE_CAST(wt_global_any.position AS INT64) = rw.position
    AND wt_global_any.tile_format = 'any'
  WHERE
    rw.section_position IS NOT NULL
),
/* Separate non-section (grid) type events */
non_section_events AS (
  SELECT
    normalized_country_code,
    experiment_slug,
    experiment_branch,
    corpus_item_id,
    raw_impression_count,
    raw_impression_count AS adjusted_impression_count, -- pass through unchanged
    report_count,
    click_count
    {% if hour_propensity_regions %},
      event_hour
    {% endif %}
  FROM
    raw_grouped_totals
  WHERE
    section_position IS NULL
),
/* Re-join events into single table */
combined_events AS (
  SELECT
    *
  FROM
    non_section_events
  UNION ALL
  SELECT
    *
  FROM
    section_events
),
{% if hour_propensity_regions %}
  /* Divide out time-of-day bias as well. Unlike position bias, which is a section-grid
     phenomenon, this applies to every row -- non-section events included. Kept in a separate
     column so only the regions listed above consume it.

     Out-of-scope rows carry a NULL event_hour, so they match no weight and this column
     equals adjusted_impression_count for them. That value is never read: only the listed
     regions select it, in experiment_region_aggregates. */
  hour_adjusted_events AS (
    SELECT
      ce.*,
      -- No global fallback, unlike the position weight: a UTC-hour curve is a local
      -- diurnal pattern shifted by the region's offset from UTC, so the global curve
      -- belongs to a different population and could correct the wrong way. A country
      -- without its own weight is left unadjusted.
      ce.adjusted_impression_count / COALESCE(
        hwt_country.weight,
        1.0
      ) AS hour_adjusted_impression_count
    FROM
      combined_events ce
    LEFT JOIN
      hour_propensity_weights hwt_country
      ON hwt_country.country = ce.normalized_country_code
      AND hwt_country.hour = ce.event_hour
  ),
{% endif %}
/* Aggregate clicks, impressions, and reports by corpus_item_id and normalized_country_code. */
aggregated_events AS (
  SELECT
    fe.corpus_item_id,
    fe.normalized_country_code,
    fe.experiment_slug,
    fe.experiment_branch,
    SAFE_CAST(SUM(adjusted_impression_count) AS INT64) AS impression_count,
    {% if hour_propensity_regions %}
      SAFE_CAST(SUM(hour_adjusted_impression_count) AS INT64) AS hour_adjusted_impression_count,
    {% endif %}
    SUM(click_count) AS click_count,
    SUM(report_count) AS report_count
  FROM
    {% if hour_propensity_regions %}
      hour_adjusted_events fe
    {% else %}
      combined_events fe
    {% endif %}
  GROUP BY
    1,
    2,
    3,
    4
),
/* Aggregate clicks, impressions, and reports across all countries. */
global_aggregates AS (
  SELECT
    corpus_item_id,
    CAST(NULL AS STRING) AS region,
    SUM(impression_count) AS impression_count,
    SUM(click_count) AS click_count,
    SUM(report_count) AS report_count
  FROM
    aggregated_events
  GROUP BY
    corpus_item_id
),
/* Aggregate clicks and impressions for country-specific ranking in Merino. */
country_aggregates AS (
  SELECT
    corpus_item_id,
    normalized_country_code AS region,
    SUM(impression_count) AS impression_count,
    SUM(click_count) AS click_count,
    SUM(report_count) AS report_count
  FROM
    aggregated_events
  WHERE
    -- Gather country (a.k.a. region) specific engagement for all countries that share a feed.
    -- https://mozilla-hub.atlassian.net/wiki/x/JY3LB
    normalized_country_code IN (
      'US',
      'CA',
      'DE',
      'CH',
      'AT',
      'GB',
      'IE',
      'BE',
      'PL',
      'FR',
      'ES',
      'IT'
    )
  GROUP BY
    corpus_item_id,
    region
),
/* Add configured experiment-specific rows using the existing region field. */
experiment_region_aggregates AS (
  SELECT
    corpus_item_id,
    CONCAT(normalized_country_code, '-', experiment_slug, '-', experiment_branch) AS region,
    {% if hour_propensity_regions %}
      -- The CONCAT is repeated from the region column above because BigQuery cannot reference
      -- a SELECT alias from a sibling expression.
      SUM(
        IF(
          CONCAT(normalized_country_code, '-', experiment_slug, '-', experiment_branch) IN UNNEST(
            {{ hour_propensity_regions }}
          ),
          hour_adjusted_impression_count,
          impression_count
        )
      ) AS impression_count,
    {% else %}
      SUM(impression_count) AS impression_count,
    {% endif %}
    SUM(click_count) AS click_count,
    SUM(report_count) AS report_count
  FROM
    aggregated_events
  WHERE
    experiment_slug IS NOT NULL
    AND experiment_branch IS NOT NULL
  GROUP BY
    corpus_item_id,
    region
),
/* Combine the "global" (no region) with the "regional" breakdown. */
combined_results AS (
  SELECT
    *
  FROM
    global_aggregates
  UNION ALL
  SELECT
    *
  FROM
    country_aggregates
  UNION ALL
  SELECT
    *
  FROM
    experiment_region_aggregates
),
removed_items AS (
  SELECT DISTINCT
    approved_corpus_item_external_id AS corpus_item_id
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.section_items_v1`
  WHERE
    event_name = 'section_item_removed'
    AND source = 'MANUAL'
    AND DATE(happened_at) > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    AND approved_corpus_item_external_id IS NOT NULL
),
filtered_results AS (
  SELECT
    cr.*
  FROM
    combined_results cr
  LEFT JOIN
    removed_items ri
    USING (corpus_item_id)
  WHERE
    NOT (ri.corpus_item_id IS NOT NULL AND cr.impression_count < 600)
)
SELECT
  *
FROM
  filtered_results
ORDER BY
  impression_count DESC
LIMIT
  -- This LIMIT was derived from the 5 MB payload size cap in Merino, the observed average
  -- record size of ~113 bytes, and recall measurements. At ~25k rows the JSON blob stays
  -- under 5 MB while preserving more lower-impression rows alongside the experiment rows.
  --
  -- Caveat: the ORDER BY now sorts across two exposure definitions, because the rows for
  -- hour_propensity_regions carry hour-adjusted counts while every other row does not. So
  -- the cut is made on mixed dimensions and can add or drop rows at the boundary. Measured
  -- offline against an artifact built without the mixed dimension: 24,998 of 25,000 rows
  -- shared, 2 added and 2 dropped, all four GB-ctrpred_engb-treatment, with the treatment
  -- (461) and control (454) row counts and both cutoff scores (510) unchanged. Small enough
  -- to leave alone, but worth re-checking if the scoped region set grows.
  25000;
