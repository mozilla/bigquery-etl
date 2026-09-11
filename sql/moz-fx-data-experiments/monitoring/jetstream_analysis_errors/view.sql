CREATE OR REPLACE VIEW
  `moz-fx-data-experiments.monitoring.jetstream_analysis_errors`
AS
-- Classifies every log row in `monitoring.logs` into a category describing
-- what kind of problem it represents, so that analysis success/failure can be
-- attributed correctly instead of relying on raw `log_level`/`exception_type`,
-- which conflate genuine jetstream defects with expected skips and benign
-- fallbacks. Covers all `source` values in `monitoring.logs` (jetstream,
-- jetstream-preview, sizing) -- the classification rules are jetstream-
-- specific, but nothing here filters by source.
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
--                           (ValidationException family + unsupported app)
--   resources           -- jetstream couldn't run at this scale: BigQuery
--                           rejected the query (Resources exceeded / Operation
--                           timed out) or a Dask worker died (KilledWorker)
--   data                -- metric values are all zero (raw or after outlier
--                           trimming), so relative uplift is undefined
--   configuration       -- config named a segment/column/app that jetstream
--                           could not resolve, or the config itself failed to
--                           load
--   valid_degradation   -- any other WARNING: jetstream detected missing
--                           data/config and fell back to a degraded but valid
--                           computation (e.g. covariate adjustment unavailable)
--   jetstream_failure   -- ERROR/CRITICAL rows not otherwise classified above:
--                           jetstream failed to compute something it should
--                           have been able to
--   informational       -- DEBUG/INFO rows (e.g. jetstream-preview, which
--                           logs at INFO)
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
      THEN 'skipped'
    WHEN message LIKE '%Resources exceeded%'
      OR message LIKE '%Operation timed out%'
      OR exception_type = 'KilledWorker'
      THEN 'resources'
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
