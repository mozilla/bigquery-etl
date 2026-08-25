"""Which experiments to analyse, and which windows each metric gets, on a given run date.

Both are pure functions of the v8 mirror plus the run date, so neither needs the Experimenter API
and neither depends on anything the aggregation does. That is what lets the rest of the job be
tested against a fixed list of slugs.
"""

import datetime
from dataclasses import dataclass

from google.cloud import bigquery

MIRROR = "moz-fx-data-experiments.monitoring.experimenter_experiments_v1"

# The furthest into a unit's own tenure any window reaches. Windows are declared as a rule rather
# than as bounds so the series never terminates, but "never terminates" and "runs for two years" are
# different things: a handful of recipes are left live long after anyone reads them, and the rule
# keeps generating weekly windows for as long as they run. The far ones are also the emptiest,
# since a window near the end of a long experiment has only the units that enrolled in its first
# few days, so the tail costs the most cells and carries the least data. Capping the horizon says the interesting
# result is a unit's first six months, which is also what the analysis windows people read are.
MAX_WINDOW_DAYS = 180

# Recipes older than this are not analysed at all. Windows are anchored to each unit's own
# enrollment, so an experiment's scan reaches back to its start no matter how short the windows
# are, and a recipe left live for years is therefore unboundedly expensive. Past a year the ones
# that remain are overwhelmingly abandoned rather than long-running: forgotten configuration whose
# results nobody has read in months. Long-running experiments that DO matter, the multi-month
# holdbacks, sit comfortably inside a year. This is a deliberate refusal, so it is reported as a
# skip rather than silently dropped.
MAX_EXPERIMENT_AGE_DAYS = 365

# Desktop only for the proof of concept, and experiments only: rollouts have no randomized control
# to compare against until the synthetic-control work lands, and Jetstream skips them today, so
# including them would be new analysis rather than a port.
APP_NAME = "firefox_desktop"

DISCOVERY_SQL = f"""
SELECT
  normandy_slug AS slug,
  start_date,
  end_date,
  reference_branch,
  ARRAY(SELECT branch.slug FROM UNNEST(branches) AS branch) AS branch_slugs
FROM `{MIRROR}`
WHERE app_name = @app_name
  AND NOT is_rollout
  AND normandy_slug IS NOT NULL
  AND start_date IS NOT NULL
  AND start_date <= @as_of
  -- Live, or ended recently enough that a final run would still be worth producing. The scheduled
  -- job passes live_only, so in production the tail is unused; it exists so an ad-hoc run can
  -- reproduce Jetstream's selection rule, which analyses an experiment for a period after it ends.
  AND (end_date IS NULL OR DATE_ADD(end_date, INTERVAL @tail_days DAY) >= @as_of)
-- Shortest first, which is what makes `limit` select the cheapest experiments to analyse rather
-- than an arbitrary set. Duration is the cost proxy: it sets how far back the shared scan reaches.
ORDER BY DATE_DIFF(@as_of, start_date, DAY) ASC
"""


@dataclass(frozen=True)
class Experiment:
    """One recipe to analyse, with everything the SQL needs and nothing it does not."""

    slug: str
    start_date: datetime.date
    end_date: datetime.date | None
    reference_branch: str
    treatment_branches: tuple[str, ...]

    @property
    def branches(self):
        """Return every branch, reference first."""
        return (self.reference_branch, *self.treatment_branches)

    def too_old(self, as_of):
        """Whether this recipe is past the age at which it is worth analysing at all."""
        return self.tenure_days(as_of) >= MAX_EXPERIMENT_AGE_DAYS

    def tenure_days(self, as_of):
        """Calendar days from first enrollment to the last date with complete data.

        The maturity frontier is derived from this: a window can only be reported once some unit has
        lived through it, and no unit can have lived longer than the experiment has run.
        """
        return (as_of - self.start_date).days


def discover(client, as_of, tail_days=90, limit=None, only_slugs=None, live_only=False):
    """Experiments to analyse on `as_of`, read from the mirror.

    `reference_branch` is occasionally absent in the mirror, and a comparison needs one, so those
    are skipped rather than guessed at: picking a reference silently would invert the sign of every
    result for that experiment.
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("app_name", "STRING", APP_NAME),
            bigquery.ScalarQueryParameter("as_of", "DATE", as_of),
            bigquery.ScalarQueryParameter("tail_days", "INT64", tail_days),
        ]
    )
    experiments, skipped = [], []
    for row in client.query(DISCOVERY_SQL, job_config=job_config).result():
        if only_slugs and row.slug not in only_slugs:
            continue
        # The 90-day tail mirrors Jetstream's selection rule, so a parallel run compares the same
        # population. It roughly triples the count, because most of it is finished experiments, so
        # `live_only` is the cheaper choice when the point is to exercise the job rather than to
        # produce results anyone reads.
        if live_only and row.end_date is not None:
            continue
        others = tuple(b for b in row.branch_slugs if b != row.reference_branch)
        if not row.reference_branch or not others:
            skipped.append(
                (row.slug, f"reference={row.reference_branch!r} others={others}")
            )
            continue
        experiment = Experiment(
            slug=row.slug,
            start_date=row.start_date,
            end_date=row.end_date,
            reference_branch=row.reference_branch,
            treatment_branches=others,
        )
        if experiment.too_old(as_of):
            skipped.append(
                (
                    row.slug,
                    f"{experiment.tenure_days(as_of)}d old, past the "
                    f"{MAX_EXPERIMENT_AGE_DAYS}d limit",
                )
            )
            continue
        experiments.append(experiment)
    return (experiments[:limit] if limit else experiments), skipped


# ------------------------------------------------------------------------ windows ----


@dataclass(frozen=True)
class Window:
    """One concrete analysis window, in days since a unit's own anchor date.

    `start` and `end` are inclusive tenure day offsets. `label` is stable across runs so a window
    keeps its identity as the series grows.
    """

    label: str
    start: int
    end: int
    kind: str

    @property
    def length(self):
        """Return the window's length in days, both ends inclusive."""
        return self.end - self.start + 1


def generate_windows(rule, tenure_days):
    """Concrete windows for one metric's window rule, up to the maturity frontier.

    Windows are declared as a length and a kind rather than as enumerated bounds, because enumerated
    bounds terminate the series: a metric declaring four weekly windows goes quiet after week four
    even though the experiment runs for months. Generating from a rule means the series extends for
    as long as the experiment does.

    Only windows some unit could have completed are generated, so there is no window here that could
    not be computed. A window needs `end + 1` days of tenure to be complete.
    """
    length, kind = rule["length"], rule["kind"]
    windows, index = [], 1
    while True:
        if kind == "disjoint":
            start, end = (index - 1) * length, index * length - 1
        elif kind == "cumulative":
            start, end = 0, index * length - 1
        else:
            raise ValueError(
                f"unknown window kind {kind!r}; expected disjoint or cumulative"
            )
        if end + 1 > tenure_days:
            return windows
        prefix = "week" if kind == "disjoint" else "cum"
        windows.append(
            Window(label=f"{prefix}:{index}", start=start, end=end, kind=kind)
        )
        index += 1
        if index > 1000:  # a runaway guard, not a real limit
            raise RuntimeError(f"window generation did not terminate for rule {rule}")


def windows_for(metric, tenure_days):
    """Every window a metric declares, in a stable order, out to the horizon."""
    reach = min(tenure_days, MAX_WINDOW_DAYS)
    return [
        window
        for rule in metric.window_rules
        for window in generate_windows(rule, reach)
    ]
