"""The Highwind daily analysis, top down.

Reading order is this file, then the modules it calls into:

    discovery.py        which experiments, and which windows each metric gets
    metrics.py          SECTION 1: metric definitions
    sql_generation.py   SECTION 2: SQL generation
    sql_running.py      SECTION 3: SQL running
    gbstats_compute.py  SECTION 4: gbstats compute
    output_writing.py   SECTION 5: output writing

The scheduled entry point is
sql/moz-fx-data-experiments/monitoring/highwind_statistics_v1/query.py, which parses the arguments
Airflow passes and calls `run_daily_job` here.

Desktop experiments only, guardrail metrics only, full recompute every run. Each of those is a
deliberate limit of the proof of concept and is noted where it bites.
"""

import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage  # type: ignore
from google.cloud import bigquery

from . import discovery, gbstats_compute
from . import metrics as metric_definitions_module
from . import output_writing, sql_generation, sql_running

# The covariate window: days before a unit's enrollment used to predict its outcome. Fixed rather
# than matched to each window's length, so one pre-period serves every window of a metric.
COVARIATE_DAYS = 28


def run_daily_job(
    as_of,
    only_slugs=None,
    limit=None,
    dry_run=False,
    workers=8,
    live_only=False,
    sample_percent=None,
    outputs=None,
):
    """Analyse every discoverable experiment for one run date.

    The expensive part is four queries for the whole run, not four per experiment: one cohort scan
    and one per source, each grouped by slug. Every experiment's date range overlaps almost every
    other's, so a per-experiment scan would read the same calendar day once per experiment covering
    it.

    The cost of sharing them is failure isolation on the query step: a source query that fails fails
    every experiment rather than one. That is a fair description of reality, since a broken source
    query IS broken for everyone, and it is the case `systemic_failure` exists to catch. Isolation
    still holds for the per-experiment statistics below, which is where experiment-specific faults
    arise.
    """
    outputs = outputs or output_writing.Outputs()
    client, storage_client = bigquery.Client(project="mozdata"), storage.Client()
    experiments, skipped = discovery.discover(
        client, as_of, limit=limit, only_slugs=only_slugs, live_only=live_only
    )
    report_selection(experiments, skipped, as_of)
    if not experiments:
        return []

    metrics_by_source = metric_definitions_module.metric_definitions()
    windows_by_slug = {
        e.slug: run_windows(e, as_of, metrics_by_source) for e in experiments
    }
    run = sql_generation.Run(
        experiments, windows_by_slug, as_of, COVARIATE_DAYS, sample_percent
    )

    cells_by_slug, timings, failure = gather_sufficient_statistics(
        client, run, metrics_by_source, as_of, dry_run
    )

    # No lock: `as_completed` yields in this thread, so the accumulation below is single-threaded.
    outcomes, results_by_slug, done = [], {}, 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                analyze_experiment,
                storage_client,
                experiment,
                as_of,
                windows_by_slug[experiment.slug],
                metrics_by_source,
                cells_by_slug.get(experiment.slug, {}),
                failure,
                outputs,
                dry_run,
            )
            for experiment in experiments
        ]
        for future in as_completed(futures):
            outcome, results = future.result()
            outcomes.append(outcome)
            results_by_slug[outcome["slug"]] = results
            done += 1
            report_progress(outcome, done, len(experiments))

    partial = bool(only_slugs or limit)
    if partial:
        # A filtered run is not the day's complete output, and the write replaces the whole
        # partition, so letting it through would delete every experiment the filter excluded. Its
        # blobs are still written, since those are keyed per slug and so only touch what it
        # analysed.
        print(
            f"\nfiltered run ({len(experiments)} experiments): blobs written, tables skipped "
            f"so the {as_of} partition keeps the full run's rows"
        )
    elif not dry_run:
        # After the loop, not inside it. Every experiment's rows share one daily partition, so a
        # per-experiment write could only append, and appending makes a retried run double its own
        # output. Writing the run together lets the load replace the partition instead.
        written = output_writing.write_tables(
            client, as_of, results_by_slug, cells_by_slug, outputs
        )
        print(
            f"\nwrote {written[0]:,} statistics rows and {written[1]:,} "
            f"sufficient-statistics rows for {as_of}"
        )
    report_run(outcomes, timings)
    if not dry_run:
        report_anomalies(outcomes)
    return outcomes


def gather_sufficient_statistics(client, run, metrics_by_source, as_of, dry_run):
    """Run the shared scan: cohort once, then one query per source, split by slug.

    Returns the cells keyed by slug plus whatever went wrong, rather than raising, so a source
    failure becomes an error grid on every experiment instead of an exception that ends the run
    having written nothing.
    """
    if not run.experiments:
        return {}, [], None
    try:
        cohort_table, cohort_timing = sql_running.materialize_cohort(
            client, sql_generation.cohort_query(run), as_of, dry_run
        )
        queries = sql_generation.build_queries(run, metrics_by_source, cohort_table)
        cells_by_slug, timings = sql_running.run_queries(
            client, queries, dry_run=dry_run
        )
        return cells_by_slug, [cohort_timing, *timings], None
    # Recorded against every experiment rather than raised, so a source failure becomes an error
    # grid instead of an exception that ends the run having written nothing.
    except Exception as error:  # noqa: BLE001
        traceback.print_exc()
        return {}, [], error


def report_progress(outcome, done, total):
    """One line per finished experiment.

    Flags on the outcome's error as well as on error cells. An experiment that failed before
    producing any cells has an error but no error cells, so keying only on cell states reports the
    worst outcome as `ok`.
    """
    flag = "ERROR" if (outcome["states"].get("error") or outcome["error"]) else "ok   "
    reason = f"  {outcome['error']}" if outcome["error"] else ""
    print(
        f"[{done:3d}/{total}] {flag} {outcome['slug'][:52]:52s} "
        f"{outcome['cells']:5d} cells{reason}"
    )


def analyze_experiment(
    storage_client,
    experiment,
    as_of,
    windows,
    metrics_by_source,
    cells,
    shared_failure,
    outputs,
    dry_run=False,
):
    """One experiment's statistics and outputs, from cells the shared scan already produced.

    Wrapped so a failure is recorded against this experiment's cells and the loop continues. The
    unit of isolation is the experiment because that is the unit a user reads: one experiment whose
    statistics blow up should not cost the others their results.
    """
    try:
        if shared_failure is not None:
            return failed_outcome(
                storage_client,
                experiment,
                as_of,
                windows,
                metrics_by_source,
                shared_failure,
                outputs,
                dry_run,
            )
        if not windows:
            # Younger than the shortest window, so no unit has completed one and there is nothing
            # to compute. Its cells are all `not_started` by construction.
            return experiment_outcome(experiment, windows, []), []
        results = gbstats_compute.compute_statistics(
            experiment, all_metrics(metrics_by_source), windows, cells
        )
        if not dry_run:
            output_writing.write_blob(
                storage_client, experiment, as_of, results, outputs
            )
        return experiment_outcome(experiment, windows, results), results
    # The whole point is to record it, not to raise.
    except Exception as error:  # noqa: BLE001
        try:
            return failed_outcome(
                storage_client,
                experiment,
                as_of,
                windows,
                metrics_by_source,
                error,
                outputs,
                dry_run,
            )
        except Exception as while_recording:  # noqa: BLE001
            # A failure while recording a failure must not propagate: it would come back out of
            # future.result() and cost every other experiment its results, which is exactly the
            # isolation this design claims to have.
            traceback.print_exc()
            return (
                experiment_outcome(
                    experiment,
                    [],
                    [],
                    None,
                    error=f"{error} (recording it also failed: {while_recording})",
                ),
                [],
            )


def failed_outcome(
    storage_client,
    experiment,
    as_of,
    windows,
    metrics_by_source,
    error,
    outputs,
    dry_run,
):
    """Record a query-level failure across every cell this experiment expected to produce.

    Without this the error rate cannot see an outage: an experiment that produced nothing would
    write no rows and so contribute nothing to the denominator, making total failure look like
    perfect health.
    """
    results = gbstats_compute.compute_statistics(
        experiment, all_metrics(metrics_by_source), windows, cells={}, failure=error
    )
    if not dry_run:
        try:
            output_writing.write_blob(
                storage_client, experiment, as_of, results, outputs
            )
        except Exception:  # noqa: BLE001
            # Recording the error grid must not depend on the thing that just failed. The grid is
            # returned either way, so the failure stays visible in this run's accounting and in the
            # table written at the end, even when the blob could not be written.
            traceback.print_exc()
    return experiment_outcome(experiment, windows, results, error=error), results


def run_windows(experiment, as_of, metrics_by_source=None):
    """List every distinct window any metric declares, out to the maturity frontier."""
    tenure = experiment.tenure_days(as_of)
    metrics_by_source = (
        metrics_by_source or metric_definitions_module.metric_definitions()
    )
    seen, windows = set(), []
    for metric in all_metrics(metrics_by_source):
        for window in discovery.windows_for(metric, tenure):
            if window.label not in seen:
                seen.add(window.label)
                windows.append(window)
    return windows


def all_metrics(metrics_by_source):
    """Flatten the per-source metric mapping into one list."""
    return [metric for metrics in metrics_by_source.values() for metric in metrics]


def experiment_outcome(experiment, windows, results, error=None):
    """Summarise what this experiment produced, for the run report and the health measurement.

    Carries no scan cost: the scan is shared across the run, so bytes and slot-hours are properties
    of the run and are reported there.
    """
    return dict(
        slug=experiment.slug,
        cells=len(results),
        states=output_writing.state_counts(results),
        error=None if error is None else f"{type(error).__name__}: {error}"[:200],
    )


def report_selection(experiments, skipped, as_of):
    """Print which experiments the run will analyse, and why the others were skipped."""
    print(
        f"as_of {as_of}: {len(experiments)} experiments to analyse, {len(skipped)} skipped"
    )
    for slug, why in skipped:
        print(f"   skipped {slug}: {why}")


def report_run(outcomes, timings):
    """Print the per-run operational summary: what it cost and how many cells failed.

    Cost is a property of the run, not of an experiment: one pass over each source serves all of
    them, so there is no per-experiment byte count to report.
    """
    cells = sum(outcome["cells"] for outcome in outcomes)
    errors = sum(outcome["states"].get("error", 0) for outcome in outcomes)
    failed = sum(1 for outcome in outcomes if outcome["error"])
    print(
        f"\n{len(outcomes)} experiments ({failed} failed outright), {cells:,} cells, "
        f"{sum(t['gb_scanned'] for t in timings) / 1000:.2f} TB scanned, "
        f"{sum(t['slot_hours'] or 0 for t in timings):.1f} slot-hours "
        f"over {len(timings)} queries"
    )
    for timing in timings:
        print(
            f"   {timing['source']:14s} {timing['wall_s']:8.1f}s "
            f"{timing['gb_scanned'] / 1000:7.2f} TB "
            f"{timing['slot_hours'] if timing['slot_hours'] is not None else '-'} slot-h "
            f"{timing['rows']:,} rows"
        )
    print(
        f"error rate {errors / cells:.2%} ({errors:,} of {cells:,} cells)"
        if cells
        else "no cells produced"
    )


def systemic_failure(outcomes):
    """Report whether the run failed as a whole, rather than losing individual experiments.

    Keyed on cell STATE, not on how many cells there are. A failing experiment still produces a
    full grid, every cell of it an error, precisely so the failure is visible in the output rather
    than absent from it; counting cells therefore cannot distinguish a total outage from a good run.

    Per-experiment isolation means one bad experiment must not fail the run, so this asks whether
    ANY experiment produced a cell in a state other than error. A quiet day on which every
    experiment is simply too young to have matured a window produces no cells and no errors, and is
    not a failure.
    """
    if not outcomes:
        return False
    produced = any(
        count
        for outcome in outcomes
        for state, count in outcome["states"].items()
        if state != gbstats_compute.ERROR
    )
    failed = any(
        outcome["error"] or outcome["states"].get(gbstats_compute.ERROR)
        for outcome in outcomes
    )
    return failed and not produced


def report_anomalies(outcomes):
    """Results that ran without erroring but do not look like an analysis.

    Worth reporting separately because the failure that cost us most in this prototype produced no
    error at all: an identifier mismatch made every metric sum to zero across a full cohort, and the
    run reported perfect health. A clean error rate is not evidence the numbers mean anything.
    """
    suspicious = []
    for outcome in outcomes:
        states = outcome["states"]
        if outcome["error"]:
            suspicious.append((outcome["slug"], f"failed: {outcome['error'][:90]}"))
            continue
        if outcome["cells"]:
            if states.get("not_started", 0) == outcome["cells"]:
                suspicious.append(
                    (outcome["slug"], "every cell not_started: cohort or join empty")
                )
            elif states.get("insufficient_data", 0) == outcome["cells"]:
                suspicious.append((outcome["slug"], "every cell insufficient_data"))
        if outcome["cells"] == 0:
            suspicious.append((outcome["slug"], "no cells: no window has matured yet"))
    if suspicious:
        print(f"\n{len(suspicious)} experiments to look at:")
        for slug, why in suspicious:
            print(f"   {slug[:56]:56s} {why}")
