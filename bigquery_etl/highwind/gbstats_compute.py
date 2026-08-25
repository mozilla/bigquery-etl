"""SECTION 4: GBSTATS COMPUTE.

Turns per-branch sufficient statistics into one result per (metric, window, comparison), each in
exactly one state.

Every cell in the expected grid gets a result, including cells nothing was computed for. That is
load-bearing rather than tidy: without it, an experiment whose queries failed writes no rows and
therefore reports a zero error rate, so the worst failure would be the most invisible.
"""

from gbstats.frequentist.tests import SequentialConfig, SequentialTwoSidedTTest
from gbstats.models.statistics import RegressionAdjustedStatistic, SampleMeanStatistic

# A window's estimator needs at least this many matured units per branch to be attempted. Purely
# mechanical: a variance wants more than one observation, and below that the estimator divides by
# zero rather than returning a wide interval.
MIN_UNITS = 2

ERROR = "error"
NOT_STARTED = "not_started"
INSUFFICIENT_DATA = "insufficient_data"
FORMING = "forming"
CONFIDENT = "confident"


def compute_statistics(experiment, metrics, windows, cells, failure=None):
    """One result per (metric, window, treatment branch), covering the whole expected grid.

    `failure` short-circuits every cell to `error`, which is how a query-level failure is recorded
    against the experiment's declared cells rather than vanishing.
    """
    results = []
    for metric in metrics:
        for window in windows_for_metric(metric, windows):
            for treatment in experiment.treatment_branches:
                results.append(
                    compute_cell(experiment, metric, window, treatment, cells, failure)
                )
    return results


def compute_cell(experiment, metric, window, treatment, cells, failure):
    """One cell: its state, and its interval when it has one."""
    identity = dict(
        metric=metric.name,
        window=window.label,
        window_kind=window.kind,
        window_start=window.start,
        window_end=window.end,
        branch=treatment,
        reference_branch=experiment.reference_branch,
    )
    if failure:
        return dict(identity, state=ERROR, error=str(failure)[:500])

    reference = cells.get((metric.name, window.label, experiment.reference_branch))
    candidate = cells.get((metric.name, window.label, treatment))
    if reference is None or candidate is None:
        return dict(identity, state=NOT_STARTED)
    if min(reference["n"], candidate["n"]) < MIN_UNITS:
        return dict(
            identity,
            state=INSUFFICIENT_DATA,
            n_reference=reference["n"],
            n_treatment=candidate["n"],
        )

    try:
        interval = sequential_interval(reference, candidate)
    except Exception as error:  # noqa: BLE001 - the state IS the error classification
        return dict(
            identity, state=ERROR, error=f"{type(error).__name__}: {error}"[:500]
        )
    if interval["point"] is None:
        return dict(
            identity,
            state=INSUFFICIENT_DATA,
            n_reference=reference["n"],
            n_treatment=candidate["n"],
        )
    state = CONFIDENT if excludes_zero(interval) else FORMING
    return dict(
        identity,
        state=state,
        n_reference=reference["n"],
        n_treatment=candidate["n"],
        **interval,
    )


def sequential_interval(reference, treatment):
    """Compute the always-valid relative interval for one comparison, as percentages.

    theta is pooled over exactly the two arms being compared and applied to both, so the adjustment
    is identical on each side of the contrast. A theta fitted within one arm would be a function of
    that arm's outcome and could bias the difference it is meant to sharpen.
    """
    theta = pooled_theta([reference, treatment])
    test = build_test(adjusted(reference, theta), adjusted(treatment, theta))
    result = test.compute_result()
    if result.expected is None or result.ci is None:
        return dict(point=None, lower=None, upper=None, theta=theta)
    return dict(
        point=result.expected * 100,
        lower=result.ci[0] * 100,
        upper=result.ci[1] * 100,
        theta=theta,
    )


def build_test(reference_statistic, treatment_statistic):
    """Construct the sequential test for one pair of arms.

    Written against the pinned gbstats signature directly rather than detected at import. Sniffing
    it from a rendered type annotation fails silently if upstream restyles its annotations, and the
    failure would surface as every cell in the run erroring rather than as one exception at startup.
    """
    return SequentialTwoSidedTTest(
        [(reference_statistic, treatment_statistic)],
        SequentialConfig(difference_type="relative"),
    )


def pooled_theta(arms):
    """Fit the CUPED slope, cov(pre, post) / var(pre), pooled over the arms passed in.

    Falls back to no adjustment when the covariate has no variance, rather than dividing by zero.
    """
    n = sum(arm["n"] for arm in arms)
    if n < 2:
        return 0.0
    sum_post = sum(arm["sum"] for arm in arms)
    sum_pre = sum(arm["pre_sum"] for arm in arms)
    pre_variance = (sum(arm["pre_sumsq"] for arm in arms) - sum_pre * sum_pre / n) / (
        n - 1
    )
    if pre_variance <= 0:
        return 0.0
    covariance = (sum(arm["xp"] for arm in arms) - sum_pre * sum_post / n) / (n - 1)
    return covariance / pre_variance


def adjusted(cell, theta):
    """One arm's sufficient statistics as a CUPED-adjusted sample mean.

    gbstats forms the adjusted mean and variance from these six numbers alone; nothing per-unit is
    needed, which is the property the whole pipeline is built around.
    """
    return RegressionAdjustedStatistic(
        n=cell["n"],
        post_statistic=SampleMeanStatistic(
            n=cell["n"], sum=cell["sum"], sum_squares=cell["sumsq"]
        ),
        pre_statistic=SampleMeanStatistic(
            n=cell["n"], sum=cell["pre_sum"], sum_squares=cell["pre_sumsq"]
        ),
        post_pre_sum_of_products=cell["xp"],
        theta=theta,
    )


def excludes_zero(interval):
    """Report whether the interval lies wholly above or wholly below zero."""
    return interval["lower"] > 0 or interval["upper"] < 0


def windows_for_metric(metric, windows):
    """Select the windows this metric declares, out of the run's full window set."""
    kinds = {rule["kind"] for rule in metric.window_rules}
    return [window for window in windows if window.kind in kinds]
