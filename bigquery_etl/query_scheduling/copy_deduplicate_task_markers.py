"""Generate the copy_deduplicate_task_markers DAG.

Generate an intermediate DAG between copy_deduplicate in the telemetry-airflow repo and
its dependent DAGs.  This DAG contains `ExternalTaskMarker`s for the downstream tasks/DAGs so
that setting state on the upstream copy_deduplicate tasks will propagate to downstream tasks.
This allows the task markers to stay updated without needing to change the code in telemetry-airflow.
"""

from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from black import FileMode, format_file_contents
from jinja2 import Environment, PackageLoader

from bigquery_etl.query_scheduling.task import EXTERNAL_TASKS
from bigquery_etl.query_scheduling.utils import (
    TELEMETRY_ALERTS_EMAIL,
    schedule_interval_delta,
)

TASK_MARKERS_DAG_NAME = "copy_deduplicate_task_markers"
TASK_MARKERS_TEMPLATE = "copy_deduplicate_task_markers.j2"
SOURCE_DAG_NAME = "copy_deduplicate"


def _execution_delta_seconds(delta_string: str) -> int:
    """Parse a `<int>s` timedelta string into an int number of seconds."""
    if delta_string is None:
        return 0
    if not delta_string.endswith("s"):
        raise ValueError(
            f"schedule_interval_delta should end with 's', got {delta_string!r}"
        )
    return int(delta_string[:-1])


def build_markers_context(dag_collection) -> dict:
    """Build the Jinja context used to render the task-markers DAG.

    Walks every task in `dag_collection`, finds upstream deps pointing at
    `copy_deduplicate`, and groups consumers by source task id.
    """
    # {task_id: schedule_interval} for each copy_deduplicate task
    source_schedules = {
        task_ref.task_id: task_ref.schedule_interval
        for task_ref in EXTERNAL_TASKS
        if task_ref.dag_name == SOURCE_DAG_NAME
    }
    if not source_schedules:
        raise ValueError(
            f"No {SOURCE_DAG_NAME} tasks found in EXTERNAL_TASKS; "
            f"cannot generate {TASK_MARKERS_DAG_NAME} DAG."
        )

    # {source_task_id: {downstream_dag_name: execution_delta_seconds}}
    consumers_by_source: Dict[str, Dict[str, int]] = defaultdict(dict)

    for dag in dag_collection.dags:
        downstream_schedule = dag.schedule_interval
        for task in dag.tasks:
            deps = (task.upstream_dependencies or []) + (task.depends_on or [])
            for dep in deps:
                if dep.dag_name != SOURCE_DAG_NAME:
                    continue
                source_schedule = source_schedules.get(dep.task_id)
                if source_schedule is None:
                    raise ValueError(
                        f"unknown {SOURCE_DAG_NAME} task_id {dep.task_id!r} in DAG"
                    )
                delta_string = schedule_interval_delta(
                    source_schedule, downstream_schedule
                )
                if delta_string is None:
                    raise ValueError(
                        f"Cannot compute execution_delta between {SOURCE_DAG_NAME} ({source_schedule!r}) and "
                        f"{dag.name} ({downstream_schedule!r})."
                    )
                delta_seconds = _execution_delta_seconds(delta_string)
                consumers_by_source[dep.task_id].setdefault(dag.name, delta_seconds)

    sources: List[tuple] = []
    for source_task_id in sorted(consumers_by_source):
        consumers = [
            {"dag_name": dag_name, "execution_delta_seconds": delta_seconds}
            for dag_name, delta_seconds in sorted(
                consumers_by_source[source_task_id].items()
            )
        ]
        sources.append((source_task_id, consumers))

    # Use copy_deduplicate's own schedule so execution_dates line up 1:1
    markers_schedule = next(iter(source_schedules.values()))

    return {
        "name": TASK_MARKERS_DAG_NAME,
        "source_dag_id": SOURCE_DAG_NAME,
        "schedule_interval": markers_schedule,
        "owner": TELEMETRY_ALERTS_EMAIL,
        "email": [TELEMETRY_ALERTS_EMAIL],
        "start_date_year": 2019,
        "start_date_month": 7,
        "start_date_day": 25,
        "tags": [
            "impact/tier_1",
            "repo/bigquery-etl",
        ],
        "sources": sources,
    }


def render_task_markers_dag(dag_collection) -> str:
    """Render the task-markers DAG."""
    env = Environment(
        loader=PackageLoader("bigquery_etl", "query_scheduling/templates"),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template(TASK_MARKERS_TEMPLATE)
    context = build_markers_context(dag_collection)
    rendered = template.render(context)
    return format_file_contents(rendered, fast=False, mode=FileMode())


def write_task_markers_dag(dag_collection, output_dir) -> Path:
    """Render and write the task-markers DAG into output_dir."""
    output_file = Path(output_dir) / f"{TASK_MARKERS_DAG_NAME}.py"
    output_file.write_text(render_task_markers_dag(dag_collection))
    return output_file
