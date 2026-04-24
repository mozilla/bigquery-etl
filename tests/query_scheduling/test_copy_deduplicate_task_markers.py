"""Tests for the copy_deduplicate_task_markers generator."""

import ast
from dataclasses import dataclass, field
from typing import List

from bigquery_etl.query_scheduling.copy_deduplicate_task_markers import (
    TASK_MARKERS_DAG_NAME,
    build_markers_context,
    render_task_markers_dag,
    write_task_markers_dag,
)
from bigquery_etl.query_scheduling.dag_collection import DagCollection
from bigquery_etl.query_scheduling.task import TaskRef


@dataclass
class MockTask:
    """Minimal stand-in for Task that exposes upstream_dependencies/depends_on."""

    upstream_dependencies: List[TaskRef] = field(default_factory=list)
    depends_on: List[TaskRef] = field(default_factory=list)


@dataclass
class MockDag:
    """Minimal stand-in for a Dag with name, tasks (List[MockTask]), and schedule_interval."""

    name: str
    schedule_interval: str
    tasks: List[MockTask] = field(default_factory=list)


def build_collection():
    """Build a DagCollection with two bqetl DAGs consuming copy_deduplicate."""
    # bqetl_a runs at 02:00, waits on copy_deduplicate_all and copy_deduplicate_main_ping
    bqetl_a = MockDag(
        name="bqetl_a",
        schedule_interval="0 2 * * *",
        tasks=[
            MockTask(
                upstream_dependencies=[
                    TaskRef(
                        dag_name="copy_deduplicate",
                        task_id="copy_deduplicate_all",
                    ),
                    TaskRef(
                        dag_name="copy_deduplicate",
                        task_id="copy_deduplicate_main_ping",
                    ),
                ]
            )
        ],
    )
    # bqetl_b runs at 04:15, waits on copy_deduplicate_all only
    bqetl_b = MockDag(
        name="bqetl_b",
        schedule_interval="15 4 * * *",
        tasks=[
            MockTask(
                upstream_dependencies=[
                    TaskRef(
                        dag_name="copy_deduplicate",
                        task_id="copy_deduplicate_all",
                    )
                ]
            )
        ],
    )
    # bqetl_c has no copy_deduplicate dependency
    bqetl_c = MockDag(
        name="bqetl_c",
        schedule_interval="0 3 * * *",
        tasks=[
            MockTask(
                upstream_dependencies=[
                    TaskRef(dag_name="some_other", task_id="some_task")
                ]
            )
        ],
    )
    collection = DagCollection([])
    collection.dags = [bqetl_a, bqetl_b, bqetl_c]
    return collection


class TestCopyDeduplicateTaskMarkers:
    def test_render_task_markers_dag_produces_valid_python(self):
        """render_task_markers_dag should return a valid dag with correct task markers."""
        source = render_task_markers_dag(build_collection())

        ast.parse(source)

        # same execution_delta as ExternalTaskSensor
        assert "datetime.timedelta(seconds=3600)" in source
        assert "datetime.timedelta(seconds=11700)" in source

        # EmptyOperators are emitted per source task.
        assert 'task_id="copy_deduplicate_all_marker"' in source
        assert 'task_id="copy_deduplicate_main_ping_marker"' in source

        # Markers are emitted per (source, consumer) pair.
        assert 'task_id="bqetl_a__copy_deduplicate_all"' in source
        assert 'task_id="bqetl_b__copy_deduplicate_all"' in source
        assert 'task_id="bqetl_a__copy_deduplicate_main_ping"' in source
        # Each ExternalTaskMarker still targets the wait_for_* sensor in the downstream DAG.
        assert 'external_task_id="wait_for_copy_deduplicate_all"' in source
        assert 'external_task_id="wait_for_copy_deduplicate_main_ping"' in source

        # bqetl_c does depend on copy_deduplicate so it shouldn't appear
        assert "bqetl_c" not in source

    def test_write_task_markers_dag(self, tmp_path):
        """"""
        output_file = write_task_markers_dag(build_collection(), tmp_path)

        assert output_file.exists()
        assert output_file.name == f"{TASK_MARKERS_DAG_NAME}.py"

        contents = output_file.read_text()
        ast.parse(contents)
        assert "copy_deduplicate_task_markers" in contents
        assert "with DAG(" in contents

    def test_build_markers_context_groups_consumers_by_source(self):
        """Task markers context should get all downstream dags with correct execution_delta's."""
        ctx = build_markers_context(build_collection())

        assert ctx["name"] == TASK_MARKERS_DAG_NAME
        assert ctx["source_dag_id"] == "copy_deduplicate"
        assert ctx["schedule_interval"] == "0 1 * * *"

        sources = dict(ctx["sources"])

        # copy_deduplicate_all has two consumers (bqetl_a at 02:00, bqetl_b at 04:15).
        all_consumers = {c["dag_name"]: c for c in sources["copy_deduplicate_all"]}
        assert set(all_consumers) == {"bqetl_a", "bqetl_b"}
        # bqetl_a: 02:00 - 01:00 = 3600 seconds.
        assert all_consumers["bqetl_a"]["execution_delta_seconds"] == 3600
        # bqetl_b: 04:15 - 01:00 = 3*3600 + 15*60 = 11700 seconds.
        assert all_consumers["bqetl_b"]["execution_delta_seconds"] == 11700

        main_consumers = {
            c["dag_name"]: c for c in sources["copy_deduplicate_main_ping"]
        }
        assert set(main_consumers) == {"bqetl_a"}
        assert main_consumers["bqetl_a"]["execution_delta_seconds"] == 3600

    def test_build_markers_context_daily_alias(self):
        """The 'daily' alias should be handled by schedule_interval_delta."""
        collection = DagCollection([])
        collection.dags = [
            MockDag(
                name="bqetl_daily_alias",
                schedule_interval="daily",
                tasks=[
                    MockTask(
                        upstream_dependencies=[
                            TaskRef(
                                dag_name="copy_deduplicate",
                                task_id="copy_deduplicate_all",
                            )
                        ]
                    )
                ],
            )
        ]
        ctx = build_markers_context(collection)
        sources = dict(ctx["sources"])
        consumer = sources["copy_deduplicate_all"][0]
        assert consumer["dag_name"] == "bqetl_daily_alias"
        # 'daily' = '0 0 * * *', source copy_deduplicate = '0 1 * * *',
        # so the consumer runs 1h before the source: delta = -3600 seconds.
        assert consumer["execution_delta_seconds"] == -3600

    def test_build_markers_context_no_consumers(self):
        """Noop DAG should be built if copy_deduplicate has no downstream tasks."""
        empty_collection = DagCollection([])
        empty_collection.dags = []

        ctx = build_markers_context(empty_collection)
        assert ctx["sources"] == []

        source = render_task_markers_dag(empty_collection)
        ast.parse(source)
