CREATE OR REPLACE VIEW
  `moz-fx-data-experiments.monitoring.jetstream_analysis_errors`
AS
-- Classifies every log row in `monitoring.logs` into a category describing
-- what kind of problem it represents, so that analysis success/failure can be
-- attributed correctly instead of relying on raw `log_level`/`exception_type`,
-- which conflate genuine jetstream defects with expected skips and benign
-- fallbacks. Covers all `source` values in `monitoring.logs` (e.g. jetstream,
-- sizing, or NULL on rows before late 2023) -- the classification rules are
-- jetstream-specific, but nothing here filters by source.
--
-- Attribution rule: a failure is `jetstream_failure` if jetstream named or
-- created the thing that couldn't be found/computed (its own scratch tables,
-- its own generated SQL, its own statistics code); it's `configuration` if
-- the experiment config named something that doesn't resolve.
--
-- Categories, from most to least specific:
--   library_warning     -- mozanalysis-internal WARNINGs with no experiment
--                           attribution (metrics.py/functions.py/segments.py)
--   skipped             -- experiment intentionally not analyzed this run
--                           (ValidationException family, unsupported app, or
--                           skip=true in config)
--   resources           -- jetstream couldn't run at this scale: BigQuery
--                           rejected the query (Resources exceeded / Operation
--                           timed out) or a Dask worker died (KilledWorker)
--   insufficient_clients -- statistic intentionally not computed: the reference
--                           branch has fewer than MIN_REFERENCE_BRANCH_CLIENTS
--                           (statistics.py) clients with non-null inputs (tiny
--                           segment or misconfigured exposures); not a failure
--   data                -- the reference branch's metric values are all zero
--                           (raw or after outlier clipping), so a linear-model
--                           statistic's relative uplift is undefined
--   configuration       -- config named a segment or column that doesn't
--                           resolve, or the experiment's config failed to
--                           load/resolve (e.g. an outcome that doesn't support
--                           the experiment's platform)
--   valid_degradation   -- any other WARNING; mostly fallbacks to a degraded
--                           but valid computation (e.g. covariate adjustment
--                           unavailable), but also run-scoping notices (e.g.
--                           skipping periods during a rerun)
--   jetstream_failure   -- ERROR/CRITICAL rows not otherwise classified above:
--                           jetstream failed to compute something it should
--                           have been able to; plus time-boxed overrides for
--                           identified jetstream bugs (see below)
--   informational       -- DEBUG/INFO rows; jetstream only logs WARNING and
--                           above here, but jetstream-preview logs at INFO
--   uncategorized       -- doesn't match any rule above; nonzero counts here
--                           indicate a mismatch between the expected logs
--                           and what jetstream is producing, which likely
--                           requires an update to the CASE logic here.
SELECT
  *,
  CASE
    WHEN log_level = 'WARNING'
      AND filename IN ('metrics.py', 'functions.py', 'segments.py')
      AND experiment IS NULL
      THEN 'library_warning'
    WHEN exception_type IN (
        -- jetstream.errors.ValidationException subclasses: the experiment is
        -- intentionally not analyzed this run, not a failure to compute.
        'NoSlugException',
        'NoEnrollmentPeriodException',
        'EnrollmentNotCompleteException',
        'NoStartDateException',
        'EndedException',
        'EnrollmentLongerThanAnalysisException',
        'HighPopulationException',
        'ExplicitSkipException',
        'RolloutSkipException',
        -- not a ValidationException, but the same "not analyzed" semantics
        'UnsupportedApplicationException'
      )
      -- same as ExplicitSkipException, logged before analysis without an exception
      OR (
        filename = 'cli.py'
        AND func_name = '_experiment_configs_to_analyse'
        AND message LIKE '%skip=true in config%'
      )
      THEN 'skipped'
    WHEN message LIKE '%Resources exceeded%'
      OR message LIKE '%Operation timed out%'
      OR exception_type = 'KilledWorker'
      THEN 'resources'
    -- Time-boxed override for an upstream bug: the low-client check counted the
    -- NULL ratio column instead of numerator/denominator, so every population_ratio
    -- was skipped. Live from jetstream PR 3319 until PR 3331 (merged 2026-10-29)
    -- reached the 04:00 UTC run on 2026-10-30; bound each such clause likewise.
    WHEN message LIKE 'Not enough clients in cohort. Skipping computing statistic population_ratio %'
      AND message LIKE '%has 0 non-null%'
      AND `timestamp` >= '2026-10-20'
      AND `timestamp` < '2026-10-30 04:00:00'
      THEN 'jetstream_failure'
    -- matched on message: `apply` also logs genuine failures via logger.exception
    WHEN filename = 'statistics.py'
      AND exception_type = 'StatisticComputationException'
      AND message LIKE 'Not enough clients in cohort%'
      THEN 'insufficient_clients'
    WHEN filename = 'statistics.py'
      AND func_name = 'transform'
      AND exception_type = 'StatisticComputationException'
      THEN 'data'
    WHEN
      -- config named a segment that doesn't resolve against jetstream's
      -- metric table (aliased `m`) or covariate-adjustment metric table
      -- (aliased `during`); or a column/name the config asked for outright
      -- doesn't exist
      message LIKE '%not found inside m at%'
      OR message LIKE '%not found inside during at%'
      OR message LIKE 'Column name%not found%'
      OR message LIKE '%Unrecognized name%'
      OR (filename = 'cli.py' AND func_name = '_experiments_to_configs')
      THEN 'configuration'
    WHEN log_level = 'WARNING'
      THEN 'valid_degradation'
    WHEN log_level IN ('ERROR', 'CRITICAL')
      THEN 'jetstream_failure'
    WHEN log_level IN ('DEBUG', 'INFO')
      THEN 'informational'
    ELSE 'uncategorized'
  END AS error_category
FROM
  `moz-fx-data-experiments.monitoring.logs`
