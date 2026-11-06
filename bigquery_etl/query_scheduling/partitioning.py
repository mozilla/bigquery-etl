"""Shared logic for getting ETL partitioning info."""

from typing import Optional

from bigquery_etl.metadata.parse_metadata import Metadata, PartitionType


def get_partition_type(metadata: Optional[Metadata]) -> Optional[PartitionType]:
    """Return the time partitioning type declared in the metadata, if any."""
    if metadata and metadata.bigquery and metadata.bigquery.time_partitioning:
        return metadata.bigquery.time_partitioning.type
    return None


def resolve_partition_parameter_name(
    scheduling: dict, partition_type: Optional[PartitionType], is_script: bool = False
) -> Optional[str]:
    """Resolve the name of the query parameter identifying the partition an ETL writes to.

    An explicitly configured partition parameter is used as-is, including setting it to null to not pass a partition parameter.
    Otherwise a default based on the partition type is used, unless it wouldn't be safe to assume what the target partition is.
    """
    if "partition_parameter" in scheduling:
        return scheduling["partition_parameter"]

    if is_script or "$" in (scheduling.get("destination_table") or ""):
        return None

    match partition_type:
        case PartitionType.YEAR:
            return "submission_year"
        case PartitionType.MONTH:
            return "submission_month"
        # If there's no partitioning metadata then assume day partitioning to preserve historical behavior.
        case PartitionType.DAY | None:
            return "submission_date"
        case PartitionType.HOUR:
            return "submission_hour"
        case _:
            raise NotImplementedError(f"Unsupported partition type: {partition_type}")


def resolve_partition_parameter_type(partition_type: Optional[PartitionType]) -> str:
    """Resolve the partition query parameter type based on the partition type."""
    match partition_type:
        case PartitionType.YEAR:
            return "INTEGER"
        case PartitionType.MONTH:
            return "DATE"
        # If there's no partitioning metadata then assume day partitioning to preserve historical behavior.
        case PartitionType.DAY | None:
            return "DATE"
        case PartitionType.HOUR:
            return "TIMESTAMP"
        case _:
            raise NotImplementedError(f"Unsupported partition type: {partition_type}")
