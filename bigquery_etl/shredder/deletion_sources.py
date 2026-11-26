"""Map tables to the deletion request sources implied by their upstream stable tables.

This only needs the shredder config, mozilla-pipeline-schemas, and the probeinfo app
listing, so it can run without BigQuery access.
"""

import sys
from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping
from typing import Optional

from google.cloud.bigquery import TableReference

from bigquery_etl.cli.utils import get_glean_app_id_to_app_name_mapping
from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas
from bigquery_etl.shredder.config import (
    CLIENT_ID,
    DELETE_TARGETS,
    GLEAN_CLIENT_ID,
    GLEAN_DELETION_REQUEST_DOCTYPE,
    SHARED_PROD,
    DeleteSource,
)

GLEAN_DELETION_REQUEST_TABLE = f"{GLEAN_DELETION_REQUEST_DOCTYPE}_v1"
ADDITIONAL_DELETION_REQUESTS_TABLE = "additional_deletion_requests_v1"


def get_deletion_request_tables() -> set[str]:
    """Return stable tables in dataset.table form that are named deletion_request_v1.

    The list comes from mozilla-pipeline-schemas, which defines every stable table.
    """
    return {
        schema.stable_table
        for schema in get_stable_table_schemas()
        if schema.bq_table == GLEAN_DELETION_REQUEST_TABLE
    }


def as_source_set(sources: DeleteSource | Iterable[DeleteSource]) -> set[DeleteSource]:
    """Return configured sources as a set.

    A single source can be written as (DeleteSource(...)), which isn't a tuple.
    """
    if isinstance(sources, DeleteSource):
        return {sources}
    return set(sources)


def app_name_to_channels(
    channel_to_app_name: Mapping[str, str],
) -> dict[str, list[str]]:
    """Return app name -> channel dataset names, from get_glean_app_id_to_app_name_mapping."""
    app_channels: dict[str, list[str]] = defaultdict(list)
    for channel, app_name in channel_to_app_name.items():
        app_channels[app_name].append(channel)
    return app_channels


def expand_sources(
    sources: DeleteSource | Iterable[DeleteSource],
    app_channels: Mapping[str, Collection[str]],
    deletion_request_tables: Optional[Collection[str]] = None,
) -> set[DeleteSource]:
    """Return configured sources in the form get_associated_deletions produces.

    Per-app deletion_request views, e.g. fenix.deletion_request, are expanded into the
    deletion_request_v1 table of each channel of the app that has one. app_channels is
    the output of app_name_to_channels, and deletion_request_tables defaults to
    get_deletion_request_tables(). Pass both in when calling this for many targets.
    """
    if deletion_request_tables is None:
        deletion_request_tables = get_deletion_request_tables()

    expanded = set()
    for source in as_source_set(sources):
        if (
            source.dataset_id in app_channels
            and source.table_id == GLEAN_DELETION_REQUEST_DOCTYPE
        ):
            for channel in app_channels[source.dataset_id]:
                table = f"{channel}_stable.{GLEAN_DELETION_REQUEST_TABLE}"
                if table in deletion_request_tables:
                    expanded.add(
                        DeleteSource(
                            table=table,
                            field=source.field,
                            project=source.project,
                            conditions=source.conditions,
                        )
                    )
        else:
            expanded.add(source)
    return expanded


def get_stable_table_sources() -> dict[str, set[DeleteSource]]:
    """Return the sources configured in DELETE_TARGETS for stable tables."""
    return {
        f"{target.project}.{target.table}": as_source_set(src)
        for target, src in DELETE_TARGETS.items()
        if target.dataset_id.endswith("_stable")
    }


def get_associated_deletions(
    upstream_stable_tables: Mapping[str, Iterable[str]],
    channel_to_app_name: Optional[Mapping[str, str]] = None,
    deletion_request_tables: Optional[Collection[str]] = None,
) -> dict[str, set[DeleteSource]]:
    """Return the deletion request sources for each table based on its upstream stable tables.

    upstream_stable_tables maps a fully qualified table to the fully qualified stable
    tables it reads from. A stable table maps to its sources in DELETE_TARGETS if it is
    configured there, otherwise to its dataset's deletion_request_v1 table if it is a
    Glean table. A Glean channel also gets its additional_deletion_requests_v1 table as
    a source when that table is one of the keys of upstream_stable_tables.

    channel_to_app_name and deletion_request_tables default to the probeinfo app
    listing and get_deletion_request_tables().
    """
    if channel_to_app_name is None:
        channel_to_app_name = get_glean_app_id_to_app_name_mapping()
    glean_channel_names = set(channel_to_app_name)
    if deletion_request_tables is None:
        deletion_request_tables = get_deletion_request_tables()

    known_stable_table_sources = get_stable_table_sources()

    # stable and derived dataset -> additional deletion requests table
    additional_deletion_requests: dict[str, str] = {}
    for table in upstream_stable_tables:
        table_ref = TableReference.from_string(table)
        if table_ref.table_id == ADDITIONAL_DELETION_REQUESTS_TABLE:
            source_table = f"{table_ref.dataset_id}.{table_ref.table_id}"
            additional_deletion_requests[table_ref.dataset_id] = source_table
            additional_deletion_requests[
                table_ref.dataset_id.replace("_derived", "_stable")
            ] = source_table

    stable_table_sources: dict[str, set[DeleteSource]] = {}

    def get_stable_table_deletions(stable_table: str) -> set[DeleteSource]:
        if stable_table in known_stable_table_sources:
            return known_stable_table_sources[stable_table]

        dataset_id = TableReference.from_string(stable_table).dataset_id
        if dataset_id.removesuffix("_stable") not in glean_channel_names:
            # legacy telemetry or non-glean structured ingestion
            return set()

        sources = set()
        if f"{dataset_id}.{GLEAN_DELETION_REQUEST_TABLE}" in deletion_request_tables:
            sources.add(
                DeleteSource(
                    table=f"{dataset_id}.{GLEAN_DELETION_REQUEST_TABLE}",
                    field=GLEAN_CLIENT_ID,
                    project=SHARED_PROD,
                )
            )
        else:
            print(f"No deletion requests for {dataset_id}", file=sys.stderr)
        if dataset_id in additional_deletion_requests:
            sources.add(
                DeleteSource(
                    table=additional_deletion_requests[dataset_id],
                    field=CLIENT_ID,
                    project=SHARED_PROD,
                )
            )
        return sources

    table_deletions: dict[str, set[DeleteSource]] = {}
    for table, stable_tables in upstream_stable_tables.items():
        deletions: set[DeleteSource] = set()
        for stable_table in stable_tables:
            if stable_table not in stable_table_sources:
                stable_table_sources[stable_table] = get_stable_table_deletions(
                    stable_table
                )
            deletions |= stable_table_sources[stable_table]
        table_deletions[table] = deletions

    return table_deletions
