import datetime
from functools import partial
from unittest.mock import ANY, Mock, patch

from google.api_core.exceptions import NotFound

from bigquery_etl.shredder import delete as shredder_delete
from bigquery_etl.shredder.config import DeleteSource, DeleteTarget

wait_for_job_partial = partial(
    shredder_delete.wait_for_job,
    states={
        "proj.dataset_table_v1$20240101": "proj:US.job_id1",
    },
    task_id="proj.dataset_table_v1$20240101",
    dry_run=True,
    create_job=lambda client: Mock(job_id="proj:US.job_id2"),
    start_date=None,
    end_date=None,
    state_table=None,
)


def test_wait_for_job_failed():
    """wait_for_job should create a new job if previous job failed."""
    mock_client = Mock()

    mock_client.get_job.return_value = Mock(job_id="proj:US.job_id1", errors=[{"": ""}])

    job = wait_for_job_partial(client=mock_client)

    assert job.job_id == "proj:US.job_id2"


def test_wait_for_job_retry_expired():
    """wait_for_job should create a new job if previous job destination no longer exists."""
    mock_client = Mock()

    mock_client.get_job.return_value = Mock(
        job_id="proj:US.job_id1",
        errors=None,
        ended=datetime.datetime(year=2024, month=1, day=1),
    )
    mock_client.get_table.side_effect = NotFound("")
    job = wait_for_job_partial(
        client=mock_client,
        check_table_existence=True,
    )

    assert (
        job.job_id == "proj:US.job_id2"
    ), "New job should be created if check_table_existence=True"

    job = wait_for_job_partial(
        client=mock_client,
        check_table_existence=False,
    )

    assert (
        job.job_id == "proj:US.job_id1"
    ), "Old job should be used if check_table_existence=False"


def test_wait_for_job_succeed():
    """wait_for_job should return the previous job if it succeeded."""
    mock_client = Mock()

    mock_client.get_job.return_value = Mock(
        job_id="proj:US.job_id1",
        errors=None,
        ended=datetime.datetime(year=2024, month=1, day=1),
    )
    mock_client.get_table.side_effect = NotFound("")

    job = wait_for_job_partial(client=mock_client)

    assert job.job_id == "proj:US.job_id1"


def test_wait_for_job_new_job():
    """wait_for_job should return a new job if there's no previous attempt."""
    mock_client = Mock()

    job = wait_for_job_partial(client=mock_client, states={})

    assert job.job_id == "proj:US.job_id2"


# args for delete_from_table, delete_from_partition_with_sampling, and delete_from_partition
COMMON_DELETE_ARGS = {
    "dry_run": True,
    "priority": "INTERACTIVE",
    "source_condition": "",
    "sources": (DeleteSource(table="dataset.deletions_v1", field="client_id"),),
    "target": DeleteTarget(table="dataset.table_v1", field="client_id"),
    "use_dml": False,
    "temp_dataset": "project.tmp",
}


@patch("bigquery_etl.shredder.delete.delete_from_partition")
def test_delete_from_partition_with_sampling(mock_delete_from_partition):
    """
    delete_from_partition_with_sampling should return a function that runs
    delete_from_partition for each sample id.
    """
    base_task_id = "proj.dataset.table_v1"

    wait_for_job = shredder_delete.delete_from_partition_with_sampling(
        **COMMON_DELETE_ARGS,
        partition=shredder_delete.Partition(
            id="20240101", condition="", is_special=False
        ),
        sampling_parallelism=10,
        task_id=base_task_id,
    )

    mock_client = Mock()

    job_function = wait_for_job.keywords["create_job"]
    job_function(mock_client)

    assert mock_delete_from_partition.call_count == 100
    for i in range(100):
        mock_delete_from_partition.assert_any_call(
            **COMMON_DELETE_ARGS,
            partition=shredder_delete.Partition(
                id="20240101", condition="", is_special=False
            ),
            sample_id=i,
            clustering_fields=ANY,
            check_table_existence=True,
            task_id=f"{base_task_id}__sample_{i}",
        )


@patch("bigquery_etl.shredder.delete.list_partitions")
def test_delete_from_table_sampling(mock_list_partitions):
    """
    delete_from_table should return a task using delete_from_partition_with_sampling
    if sampling is enabled for the given table, otherwise it should return delete_from_table.
    """
    mock_list_partitions.return_value = [
        shredder_delete.Partition(id="20240101", condition="", is_special=False)
    ]

    mock_client = Mock()

    for sampling_enabled in (True, False):
        task = list(
            shredder_delete.delete_from_table(
                **COMMON_DELETE_ARGS,
                client=mock_client,
                use_sampling=True,
                sampling_parallelism=10,
                max_single_dml_bytes=1,
                partition_limit=None,
                end_date="",
            )
        )[0]
        create_job_func = task.func.keywords["create_job"]

        assert (
            create_job_func.__name__ == "delete_by_sample"
            if sampling_enabled
            else "create_job"
        )
