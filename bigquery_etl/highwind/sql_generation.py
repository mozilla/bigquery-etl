"""SECTION 2: SQL GENERATION.

One query per source table for the WHOLE RUN, not per experiment. Each returns per-branch
sufficient statistics in long format, one row per (slug, branch, window, metric), so the result's
width is fixed no matter how many experiments, metrics or windows a run covers.

Each source is read once over the union of every experiment's date range and grouped by slug. Those
ranges overlap almost entirely, so reading per experiment would read the same calendar day once per
experiment covering it. One consequence worth knowing: the SCAN is set by the longest experiment's
range rather than by how many experiments there are, so an experiment inside that range adds little
to it. The join below still grows with the cohort.

Window membership is arithmetic rather than a join. Every window is built out of one grid of
disjoint buckets, and a source row's bucket is `DIV(tenure_day, bucket_length)`, so the row is read
once no matter how many windows an experiment has. Each source query is six CTEs, of which three
do the aggregation:

    cohort          the run's assignment table, one row per (slug, unit)
    meta            per-slug facts, currently how many buckets each experiment has matured
    source_rows     the single scan, bounded to the columns this source's metrics name

    A  bucket_totals   one pass over source rows, grouped to (slug, unit, bucket), one raw
                       aggregate per metric. Metrics are columns here, bounded by the metric set,
                       and the output is smaller than its own input.
       unit_buckets    every (slug, unit, bucket) a unit has matured, whether or not it reported.
                       This is what makes `n` the cohort rather than the reporting subset of it.
    B  unit_windows    each cumulative window as a running total across buckets, which is a window
                       function over a units-by-buckets table rather than a second pass over
                       source rows. Thresholds are applied here, once, to the combined value.
    C  the rollup      unpivot metrics to rows and aggregate to branch, so the output schema stays
                       independent of the metric set.

Nothing per-unit leaves BigQuery. The per-unit stages are intermediate CTEs inside the same query
that aggregates them away.
"""

import datetime
import math
import textwrap

from .metrics import SOURCES

# The bucket a unit's pre-enrollment rows land in. The covariate is one fixed window rather than
# one per analysis window, so it is a single extra bucket that sorts before every other and is
# read back out of the same pass.
PRE_BUCKET = -1

# Granularity of the branch-balancing hash. A unit is kept when its hash modulo this falls under
# the arm's retention rate times this, so it sets how finely a rate can be expressed: one part per
# million is far below the sampling error on any arm large enough to need balancing.
BALANCE_RESOLUTION = 1_000_000

# How many times the smallest arm the largest is allowed to be. Chosen at the knee of the
# precision-versus-units curve for a 90/10 split: 4:1 discards about half the units for ~6% wider
# intervals, where 1:1 discards 80% for 34%. Above ~6:1 there is little left to save, below ~3:1
# the interval cost climbs steeply.
MAX_BRANCH_RATIO = 4

# Client sample to analyse, as a percentage, or None for every client, carried on `Run`. The source
# tables are clustered on `sample_id`, so this prunes storage rather than discarding rows after
# reading them, and the scan falls roughly in proportion to the sample. Note it is the second
# clustering key behind `normalized_channel` on two of the three, which weakens the pruning for a
# predicate on `sample_id` alone.
#
# The subtlety that makes it safe is which id the sample is taken on. `sample_id` on these tables is
# hashed from the LEGACY client id, while `events_stream`, where the cohort comes from, carries a
# `sample_id` hashed from the GLEAN id. Sampling each side on its own column would select two
# populations that barely intersect, and because the join preserves unmatched units at zero the
# result would be a quietly deflated metric rather than an error. `udf.safe_sample_id` is the
# function behind the column, verified to reproduce it on every row of a test partition, so applying
# it to the cohort's legacy id selects exactly the clients the sampled sources contain.


def sample_clause(column, sample_percent, prefix="AND "):
    """Restrict to a client sample, keyed on a legacy telemetry client id.

    Qualified, because the UDF is defined once in the shared-prod project and this job's client
    defaults to a different one.
    """
    if sample_percent is None:
        return ""
    return f"{prefix}mozdata.udf.safe_sample_id({column}) < {sample_percent}"


# ------------------------------------------------------------------------------- the run ----


class Run:
    """Everything the shared queries need to know about the experiments in one run.

    Holds the bucket grid, which is shared: the bucket LENGTH is
    shared by every experiment because every window rule is weekly, and the only thing that differs
    per experiment is how many buckets it has matured, which rides along as a per-slug column.
    """

    def __init__(
        self, experiments, windows_by_slug, as_of, covariate_days, sample_percent=None
    ):
        """Derive the run's bucket grid from the windows its experiments declare."""
        self.as_of = as_of
        self.covariate_days = covariate_days
        self.sample_percent = sample_percent
        # Only experiments with at least one matured window take part in the shared queries. A
        # younger one contributes no rows and would only widen the scan.
        self.experiments = [e for e in experiments if windows_by_slug.get(e.slug)]
        self.windows_by_slug = windows_by_slug
        every_window = [w for e in self.experiments for w in windows_by_slug[e.slug]]
        self.length = bucket_length(every_window) if every_window else 7
        self.buckets_by_slug = {
            e.slug: max((w.end + 1) // self.length for w in windows_by_slug[e.slug])
            for e in self.experiments
        }

    @property
    def slugs(self):
        """Return the slugs taking part in this run's shared queries."""
        return [e.slug for e in self.experiments]

    def earliest_start(self):
        """Return the start date of the run's oldest experiment."""
        return min(e.start_date for e in self.experiments)

    def first_source_date(self):
        """Return the oldest submission_date any experiment in the run needs.

        The union bound: one experiment's covariate reaches furthest back, and reading from there
        once serves every experiment.
        """
        return self.earliest_start() - datetime.timedelta(days=self.covariate_days)

    def meta_cte(self):
        """Per-slug facts the shared queries join against: how many buckets each has matured.

        A literal rather than a table because it is one short row per experiment, derived from the
        mirror and the run date, and inlining it lets BigQuery treat it as a broadcast side of the
        join instead of another scan.
        """
        rows = ",\n            ".join(
            f"STRUCT('{slug}' AS slug, {count} AS matured_buckets)"
            for slug, count in sorted(self.buckets_by_slug.items())
        )
        return f"SELECT * FROM UNNEST([\n            {rows}])"

    def matured_buckets(self):
        """How many whole buckets a unit has completed, capped at its experiment's horizon.

        The cap is what makes the window horizon save anything. Without it a long-tenured unit
        still generates every bucket it ever matured, and the ones past the horizon carry no
        window label, so they are built, joined, sorted and only then discarded. It also bounds the
        source rows each unit contributes, since stage A reaches only this far.
        """
        elapsed = f"DIV(DATE_DIFF(DATE '{self.as_of}', c.enrollment_date, DAY), {self.length})"
        return f"LEAST({elapsed}, m.matured_buckets)"


def bucket_length(windows):
    """Determine the disjoint bucket length every window in this run is built from.

    Validated rather than assumed. A cumulative window is summed from whole buckets, so its length
    has to be an exact multiple of the bucket length or there is no grid to sum it over, and a
    disjoint window has to BE one bucket or it cannot be read off a single row. Both current rules
    are 7 days so this holds trivially, but it is a real constraint on the window vocabulary and a
    silently wrong answer if a future rule breaks it.
    """
    disjoint = {window.length for window in windows if window.kind == "disjoint"}
    if len(disjoint) > 1:
        raise ValueError(
            f"disjoint windows must all share one length to form a bucket grid, got "
            f"{sorted(disjoint)}"
        )
    length = (
        disjoint.pop() if disjoint else math.gcd(*[window.length for window in windows])
    )
    for window in windows:
        if window.kind == "disjoint" and window.length != length:
            raise ValueError(
                f"disjoint window {window.label} is {window.length} days, but the bucket grid is "
                f"{length}; a disjoint window must be exactly one bucket"
            )
        if window.kind == "cumulative" and window.start != 0:
            raise ValueError(
                f"cumulative window {window.label} starts at {window.start}, not 0"
            )
        if window.start % length or (window.end + 1) % length:
            raise ValueError(
                f"window {window.label} [{window.start},{window.end}] does not land on the "
                f"{length}-day bucket grid; cumulative window lengths must be exact multiples of "
                f"the disjoint length"
            )
    return length


# ------------------------------------------------------------------------------ the cohort ----


def cohort_query(run):
    """Every unit's slug, branch and first enrollment date, for every experiment at once.

    One pass over `events_stream` for the whole run rather than one per experiment. Enrollment
    events for all slugs sit in the same partitions, so extracting them together and partitioning by
    slug afterwards costs one scan rather than one per experiment.

    Read from `events_stream` rather than `enrollment_status` in `nimbus_targeting_context`. The
    targeting-context ping carries an events array and fires on every Nimbus evaluation, so scanning
    it over an experiment's lifetime costs two orders of magnitude more, and this proof of concept
    only analyses experiments. Rollouts will need `enrollment_status`, since it is the only source
    that records a rollout's assignment, and that cost has to be paid then rather than avoided.

    A unit is taken at its FIRST enrollment in each experiment, so a client that re-enrols is
    anchored once. Units seen on more than one branch of the same experiment are dropped rather than
    resolved: a contradictory assignment is a data problem, and picking an arm would silently bias
    the comparison it feeds. A unit in several DIFFERENT experiments is kept in each, which is why
    the grouping is by (slug, unit) rather than by unit.

    Lopsided branches are then capped rather than equalised, per slug. A holdback splits 90/10 or
    worse, so the large arm carries most of the units the aggregation has to move while adding
    little to the precision of the contrast, whose standard error goes as sqrt(1/n_ref + 1/n_treat)
    and is dominated by the smaller arm. But "adds little" is not "adds nothing", and equalising is
    the expensive way to find that out: on a 90/10 split, cutting the large arm to 1:1 discards 80%
    of the units and widens the interval by 34%, whereas capping at 4:1 discards about half for
    roughly 6%. The knee of that curve is the point of the cap.

    The sampling is a hash of the unit id, so it is deterministic across runs and nested as the rate
    moves, which stops the cohort churning from day to day and the results jittering for reasons
    unrelated to the data. It is a no-op on any experiment already inside the ratio.
    """
    return textwrap.dedent(f"""
        WITH valid_branch AS (
        {_indent(branch_lookup(run), 10)}
        ),
        assigned AS (
        {_indent(assignment_query(run), 10)}
        ),
        branch_sizes AS (
          SELECT slug, branch, COUNT(*) AS units FROM assigned GROUP BY slug, branch
        ),
        smallest_arm AS (
          SELECT slug, MIN(units) AS smallest FROM branch_sizes GROUP BY slug
        )
        SELECT a.slug, a.unit_id, a.branch, a.enrollment_date
        FROM assigned AS a
        JOIN branch_sizes AS b USING (slug, branch)
        JOIN smallest_arm AS s USING (slug)
        WHERE MOD(ABS(FARM_FINGERPRINT(a.unit_id)), {BALANCE_RESOLUTION})
              < {BALANCE_RESOLUTION}
                * LEAST(1.0, {MAX_BRANCH_RATIO} * s.smallest / b.units)
    """).strip()


def branch_lookup(run):
    """List the (slug, branch) pairs this run recognises.

    Inlined so the enrollment scan can discard an event naming a branch the mirror does not list,
    which happens when a recipe is edited after launch, without a second pass to find out.
    """
    rows = ",\n    ".join(
        f"STRUCT('{experiment.slug}' AS slug, '{branch}' AS branch)"
        for experiment in run.experiments
        for branch in experiment.branches
    )
    return f"SELECT * FROM UNNEST([\n    {rows}])"


def assignment_query(run):
    """Each unit's branch and first enrollment date per slug, before any balancing."""
    slugs = ", ".join(f"'{slug}'" for slug in run.slugs)
    return textwrap.dedent(f"""
        SELECT
          slug,
          unit_id,
          ANY_VALUE(branch_slug) AS branch,
          MIN(enrollment_date) AS enrollment_date
        FROM (
          SELECT
            JSON_VALUE(event_extra, '$.experiment') AS slug,
            -- The legacy id, NOT the Glean `client_id`: every source this job reads
            -- (clients_daily, search_clients_engines_sources_daily, desktop_active_users) is
            -- keyed on the legacy telemetry id, and joining the Glean id against them matches
            -- nothing while still producing a full cohort, so every metric silently sums to zero.
            legacy_telemetry_client_id AS unit_id,
            JSON_VALUE(event_extra, '$.branch') AS branch_slug,
            DATE(submission_timestamp) AS enrollment_date
          FROM `mozdata.firefox_desktop.events_stream`
          WHERE DATE(submission_timestamp)
                BETWEEN '{run.earliest_start()}' AND '{run.as_of}'
            AND event_category = 'nimbus_events'
            AND event_name = 'enrollment'
            AND JSON_VALUE(event_extra, '$.experiment') IN ({slugs})
            AND legacy_telemetry_client_id IS NOT NULL
            {sample_clause("legacy_telemetry_client_id", run.sample_percent, prefix="AND ")}
        )
        JOIN valid_branch USING (slug)
        WHERE branch = branch_slug
        GROUP BY slug, unit_id
        HAVING COUNT(DISTINCT branch_slug) = 1
    """).strip()


# ----------------------------------------------------------------------- the source queries ----


def build_queries(run, metrics_by_source, cohort_table):
    """One SQL string per source table, covering every experiment in the run."""
    return {
        source: build_source_query(run, source, metrics, cohort_table)
        for source, metrics in metrics_by_source.items()
    }


def build_source_query(run, source, metrics, cohort_table):
    """Build the full query for one source: cohort, per-slug facts, rows, stages, rollup."""
    spec = SOURCES[source]
    return textwrap.dedent(f"""
        WITH cohort AS (
        {_indent(cohort_source(cohort_table))}
        ),
        meta AS (
        {_indent(run.meta_cte())}
        ),
        source_rows AS (
        {_indent(source_cte(spec, run))}
        ),
        bucket_totals AS (
        {_indent(bucket_totals_cte(metrics, run))}
        ),
        unit_buckets AS (
        {_indent(unit_buckets_cte(run))}
        ),
        unit_windows AS (
        {_indent(unit_windows_cte(metrics))}
        )
        {rollup_select(metrics)}
    """).strip()


def cohort_source(cohort_table):
    """Where the source queries read the cohort from.

    On a dry run the cohort table has deliberately not been created, so referencing it would fail
    validation for a reason that says nothing about the query being validated. An empty literal of
    the same shape lets BigQuery check everything downstream of it instead.
    """
    if cohort_table is None:
        return (
            "SELECT * FROM UNNEST(ARRAY<STRUCT<slug STRING, unit_id STRING, branch STRING, "
            "enrollment_date DATE>>[])"
        )
    return f"SELECT slug, unit_id, branch, enrollment_date FROM `{cohort_table}`"


def source_cte(spec, run):
    """Select the source rows for the whole run, over the union of every experiment's range.

    One scan serving every experiment. It starts `covariate_days` before the EARLIEST enrollment in
    the run, because the covariate window precedes assignment, and ends at the run date. The range
    reaches back to the oldest experiment's start however short the windows are, because windows are
    anchored to each unit's own enrollment rather than to the calendar: a unit that enrolled on day
    one needs its own first week, which is months ago. That is why an old recipe is expensive no
    matter how the window horizon is set, and why age is handled by declining to analyse it.

    Column selection is explicit, so the scan is the columns this source's metrics name plus
    whatever a source-level restriction reads. `desktop_active_users` is the case where those
    differ: its `is_desktop` is a computed column, so filtering on it also reads the ISP,
    distribution and version columns behind it.
    """
    columns = ",\n  ".join(spec["columns"])
    restriction = f"\n  AND {spec['where']}" if spec.get("where") else ""
    # The clustered column itself here, not `safe_sample_id(...)` of the id as in the cohort. They select the same
    # clients, but only the bare column lets BigQuery skip the blocks rather than read and discard
    # them, which is the entire point of sampling here.
    sample = (
        f"\n  AND sample_id < {run.sample_percent}"
        if run.sample_percent is not None
        else ""
    )
    return textwrap.dedent(f"""
        SELECT
          {spec['unit_column']} AS unit_id,
          submission_date,
          {columns}
        FROM `{spec['table']}`
        WHERE submission_date
          BETWEEN DATE '{run.first_source_date()}'
              AND DATE '{run.as_of}'{restriction}{sample}
    """).strip()


# ---------------------------------------------------------------------------- the stages ----


def bucket_totals_cte(metrics, run):
    """STAGE A: one row per (slug, unit, bucket), carrying one RAW aggregate per metric.

    The only stage that reads source rows, and it reads each of them once: a row's bucket is
    arithmetic on its tenure day, so nothing is replicated per window. Metrics are columns here
    rather than rows, which is safe because the metric set bounds them and they never multiply by
    the window count.

    Raw means before any threshold. A 0/1 metric carries its underlying SUM, COUNTIF or MIN through
    this stage so the threshold can be applied once to the combined value in stage B.

    The covariate rides along as one extra bucket rather than a second pass, so the pre-enrollment
    window costs nothing beyond the rows it reads.

    LEFT JOIN, and the direction is load-bearing rather than stylistic. The predicate on
    `s.submission_date` below already discards units with no matching row, so this returns exactly
    what an inner join would; measured on one source, both forms produce identical output. But an
    inner join lets BigQuery reorder the two sides, and it was measured choosing a plan that read
    orders of magnitude more records than this one, at several times the slot cost. Writing it as an
    outer join drove the planner to hash-join from the cohort instead. Treat that as an observation
    rather than a guarantee: BigQuery can rewrite an outer join to an inner one when a predicate
    rejects the null-extended rows, as this one does, so the plan is not pinned by the syntax.
    """
    aggregates = ",\n  ".join(
        f"{metric.reducer.bucket_agg} AS {metric.name}__raw" for metric in metrics
    )
    return textwrap.dedent(f"""
        SELECT
          c.slug,
          c.unit_id,
          IF(d < 0, {PRE_BUCKET}, DIV(d, {run.length})) AS bucket,
          {aggregates}
        FROM cohort AS c
        JOIN meta AS m
          ON m.slug = c.slug
        LEFT JOIN source_rows AS s
          ON s.unit_id = c.unit_id
        , UNNEST([DATE_DIFF(s.submission_date, c.enrollment_date, DAY)]) AS d
        WHERE d BETWEEN -{run.covariate_days}
                    AND {run.matured_buckets()} * {run.length} - 1
        GROUP BY c.slug, c.unit_id, bucket
    """).strip()


def unit_buckets_cte(run):
    """Every (slug, unit, bucket) a unit has matured, whether or not it reported anything.

    Generated from the cohort rather than from the source rows so a unit that reported nothing still
    appears, with its metric values 0, and so counts in `n`, which is what makes the denominator
    the cohort rather than the reporting subset of it.

    A unit younger than one bucket gets only the covariate row, because `matured_buckets` is 0 for
    it and the array then runs from the covariate bucket to -1. The rollup drops that row with
    `bucket >= 0`, which is where the maturity gate actually bites: a unit enters a window exactly
    once, when it completes it.
    """
    return textwrap.dedent(f"""
        SELECT
          c.slug,
          c.unit_id,
          c.branch,
          bucket
        FROM cohort AS c
        JOIN meta AS m
          ON m.slug = c.slug
        CROSS JOIN UNNEST(GENERATE_ARRAY({PRE_BUCKET}, {run.matured_buckets()} - 1)) AS bucket
    """).strip()


def unit_windows_cte(metrics):
    """STAGE B: each bucket's row carries the value of every window that ends on it.

    A cumulative window is the running total to this bucket, which is a window function over a
    units-by-buckets table rather than a second pass over source rows. The table is smaller than the
    source rows it came from, so the whole window series costs one sort.

    The window LABEL is arithmetic on the bucket index rather than a lookup, which is what lets one
    query serve experiments of different ages: the window ending on bucket b is always the (b+1)th
    of its family. Experiments differ only in how many buckets they reach, and that is already
    enforced upstream by `matured_buckets`.

    The metric's threshold is applied HERE, to the combined raw value, and only here. The covariate
    is the pre bucket's raw value read across the partition, so it is one value per unit rather than
    one per unit per window.
    """
    kinds = sorted({rule["kind"] for metric in metrics for rule in metric.window_rules})
    labels = "".join(
        f"  IF(g.bucket >= 0, CONCAT('{_PREFIX[kind]}:', g.bucket + 1), NULL)"
        f" AS {kind}_window,\n"
        for kind in kinds
    )
    values = ",\n  ".join(
        [
            *(
                f"{window_value(metric, kind)} AS {metric.name}__{kind}"
                for metric in metrics
                for kind in sorted({rule["kind"] for rule in metric.window_rules})
            ),
            *(f"{covariate_value(metric)} AS {metric.name}__pre" for metric in metrics),
        ]
    )
    return textwrap.dedent(f"""
        SELECT
          g.slug,
          g.branch,
          g.bucket,
        {labels}  {values}
        FROM unit_buckets AS g
        LEFT JOIN bucket_totals AS b
          ON b.slug = g.slug AND b.unit_id = g.unit_id AND b.bucket = g.bucket
        WINDOW
          unit_to_date AS (PARTITION BY g.slug, g.unit_id ORDER BY g.bucket
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
          unit_all AS (PARTITION BY g.slug, g.unit_id)
    """).strip()


# The label a window of each family carries. Kept beside the arithmetic that builds it, since the
# two have to agree with `discovery.generate_windows` for a cell to be found by the statistics.
_PREFIX = {"cumulative": "cum", "disjoint": "week"}


def window_value(metric, kind):
    """One unit's value for the window of `kind` ending on this bucket.

    Cumulative combines every bucket to date, excluding the covariate bucket, which sorts first and
    would otherwise be counted as post-enrollment data. Disjoint is the bucket itself.
    """
    reducer = metric.reducer
    if kind == "cumulative":
        combined = (
            f"{reducer.combine}(IF(g.bucket >= 0, b.{metric.name}__raw, NULL))"
            f" OVER unit_to_date"
        )
    else:
        combined = f"b.{metric.name}__raw"
    return reducer.finalize(f"COALESCE({combined}, {reducer.no_rows})")


def covariate_value(metric):
    """One unit's pre-enrollment value, read off the covariate bucket across the partition."""
    reducer = metric.reducer
    combined = (
        f"MAX(IF(g.bucket = {PRE_BUCKET}, b.{metric.name}__raw, NULL)) OVER unit_all"
    )
    return reducer.finalize(f"COALESCE({combined}, {reducer.no_rows})")


def rollup_select(metrics):
    """STAGE C: per-branch sufficient statistics, one row per (slug, branch, window, metric).

    The six aggregates are everything a covariate-adjusted mean comparison needs, which is the
    reason no per-unit row has to travel. Metrics are unpivoted through a struct array so the metric
    name becomes a row key, which is what makes the schema independent of the metric set. The array
    is applied to the units-by-buckets table rather than to source rows, so it multiplies the small
    table and not the large one.

    A NULL label is a bucket that completes no window of that metric's kind, and the covariate
    bucket is every metric's NULL, so the filter drops both.
    """
    structs = ",\n         ".join(
        f"STRUCT('{metric.name}' AS metric, {rule['kind']}_window AS window_label, "
        f"{metric.name}__{rule['kind']} AS post, {metric.name}__pre AS pre)"
        for metric in metrics
        for rule in metric.window_rules
    )
    return textwrap.dedent(f"""
        SELECT
          slug,
          branch,
          m.window_label,
          m.metric,
          COUNT(*) AS n,
          SUM(m.post) AS sum,
          SUM(POW(m.post, 2)) AS sumsq,
          SUM(m.pre) AS pre_sum,
          SUM(POW(m.pre, 2)) AS pre_sumsq,
          SUM(m.post * m.pre) AS xp
        FROM unit_windows,
             UNNEST([{structs}]) AS m
        WHERE bucket >= 0
          AND m.window_label IS NOT NULL
        GROUP BY slug, branch, m.window_label, m.metric
    """).strip()


def _indent(block, spaces=2):
    return textwrap.indent(block, " " * spaces)
