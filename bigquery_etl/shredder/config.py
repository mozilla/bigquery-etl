#!/usr/bin/env python3

"""Meta data about tables and ids for self serve deletion."""

from dataclasses import dataclass
from functools import partial
from typing import Tuple, Union
import re

from google.cloud import bigquery

from ..util.bigquery_id import qualified_table_id


SHARED_PROD = "moz-fx-data-shared-prod"
GLEAN_SCHEMA_ID = "glean_ping_1"


@dataclass(frozen=True)
class DeleteSource:
    """Data class for deletion request source."""

    table: str
    field: str
    project: str = SHARED_PROD

    @property
    def table_id(self):
        """Table Id."""
        return self.table.split(".", 1)[-1]

    @property
    def dataset_id(self):
        """Dataset Id."""
        return self.table.split(".", 1)[0]


@dataclass(frozen=True)
class DeleteTarget:
    """Data class for deletion request target.

    Rows will be removed using either one DELETE statement for the whole table,
    or one DELETE statement per partition if the table is larger than some
    configurable threshold.
    """

    table: str
    field: Union[str, Tuple[str]]
    project: str = SHARED_PROD

    @property
    def table_id(self):
        """Table Id."""
        return self.table.partition(".")[2]

    @property
    def dataset_id(self):
        """Dataset Id."""
        return self.table.partition(".")[0]

    @property
    def fields(self) -> Tuple[str]:
        """Fields."""
        if isinstance(self.field, tuple):
            return self.field
        return (self.field,)


CLIENT_ID = "client_id"
GLEAN_CLIENT_ID = "client_info.client_id"
IMPRESSION_ID = "impression_id"
USER_ID = "user_id"
POCKET_ID = "pocket_id"
SHIELD_ID = "shield_id"
ECOSYSTEM_CLIENT_ID = "payload.ecosystem_client_id"
PIONEER_ID = "payload.pioneer_id"
ID = "id"
CFR_ID = f"COALESCE({CLIENT_ID}, {IMPRESSION_ID})"
FXA_USER_ID = "jsonPayload.fields.user_id"
# these must be in the same order as SYNC_SRCS
SYNC_IDS = ("SUBSTR(payload.device_id, 0, 32)", "payload.uid")

DESKTOP_SRC = DeleteSource(
    table="telemetry_stable.deletion_request_v4", field=CLIENT_ID
)
IMPRESSION_SRC = DeleteSource(
    table="telemetry_stable.deletion_request_v4",
    field="payload.scalars.parent.deletion_request_impression_id",
)
CFR_SRC = DeleteSource(
    # inject sql via f"`{sql_table_id(source)}`" to select client_id and impression_id
    table="telemetry_stable.deletion_request_v4`,"
    f" UNNEST([{CLIENT_ID}, {IMPRESSION_SRC.field}]) AS `_",
    field="_",
)
FXA_HMAC_SRC = DeleteSource(
    table="firefox_accounts_derived.fxa_delete_events_v1", field="hmac_user_id"
)
FXA_SRC = DeleteSource(
    table="firefox_accounts_derived.fxa_delete_events_v1", field=USER_ID
)
# these must be in the same order as SYNC_IDS
SYNC_SOURCES = (
    DeleteSource(
        table="telemetry_stable.deletion_request_v4",
        field="payload.scalars.parent.deletion_request_sync_device_id",
    ),
    DeleteSource(
        table="firefox_accounts_derived.fxa_delete_events_v1",
        field="SUBSTR(hmac_user_id, 0, 32)",
    ),
)
SOURCES = [DESKTOP_SRC, IMPRESSION_SRC, CFR_SRC, FXA_HMAC_SRC, FXA_SRC] + list(
    SYNC_SOURCES
)


client_id_target = partial(DeleteTarget, field=CLIENT_ID)
glean_target = partial(DeleteTarget, field=GLEAN_CLIENT_ID)
impression_id_target = partial(DeleteTarget, field=IMPRESSION_ID)
cfr_id_target = partial(DeleteTarget, field=CFR_ID)
fxa_user_id_target = partial(DeleteTarget, field=FXA_USER_ID)
user_id_target = partial(DeleteTarget, field=USER_ID)

DELETE_TARGETS = {
    client_id_target(
        table="search_derived.mobile_search_clients_daily_v1"
    ): DESKTOP_SRC,
    client_id_target(table="search_derived.search_clients_daily_v8"): DESKTOP_SRC,
    client_id_target(table="search_derived.search_clients_last_seen_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.attitudes_daily_v1"): DESKTOP_SRC,
    client_id_target(
        table="telemetry_derived.clients_daily_histogram_aggregates_v1"
    ): DESKTOP_SRC,
    client_id_target(
        table="telemetry_derived.clients_daily_scalar_aggregates_v1"
    ): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.clients_daily_v6"): DESKTOP_SRC,
    client_id_target(
        table="telemetry_derived.clients_daily_histogram_aggregates_v1"
    ): DESKTOP_SRC,
    client_id_target(
        table="telemetry_derived.clients_daily_scalar_aggregates_v1"
    ): DESKTOP_SRC,
    client_id_target(
        table="telemetry_derived.clients_histogram_aggregates_v1"
    ): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.clients_last_seen_v1"): DESKTOP_SRC,
    client_id_target(
        table="telemetry_derived.clients_scalar_aggregates_v1"
    ): DESKTOP_SRC,
    client_id_target(
        table="telemetry_derived.clients_profile_per_install_affected_v1"
    ): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.core_clients_daily_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.core_clients_last_seen_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.event_events_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.experiments_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.main_events_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.main_summary_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.block_autoplay_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.crash_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.downgrade_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.event_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.first_shutdown_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.focus_event_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.frecency_update_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.health_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.heartbeat_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.main_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.modules_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.new_profile_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.saved_session_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.shield_icq_v1_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.shield_study_addon_v3"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.shield_study_error_v3"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.shield_study_v3"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.testpilot_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.third_party_modules_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.untrusted_modules_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.update_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.voice_v4"): DESKTOP_SRC,
    # activity stream
    cfr_id_target(table="messaging_system_stable.cfr_v1"): CFR_SRC,
    cfr_id_target(table="messaging_system_derived.cfr_users_daily_v1"): CFR_SRC,
    cfr_id_target(table="messaging_system_derived.cfr_users_last_seen_v1"): CFR_SRC,
    client_id_target(table="activity_stream_stable.events_v1"): DESKTOP_SRC,
    client_id_target(table="messaging_system_stable.onboarding_v1"): DESKTOP_SRC,
    client_id_target(table="messaging_system_stable.snippets_v1"): DESKTOP_SRC,
    client_id_target(table="activity_stream_stable.sessions_v1"): DESKTOP_SRC,
    client_id_target(
        table="messaging_system_derived.onboarding_users_daily_v1"
    ): DESKTOP_SRC,
    client_id_target(
        table="messaging_system_derived.onboarding_users_last_seen_v1"
    ): DESKTOP_SRC,
    client_id_target(
        table="messaging_system_derived.snippets_users_daily_v1"
    ): DESKTOP_SRC,
    client_id_target(
        table="messaging_system_derived.snippets_users_last_seen_v1"
    ): DESKTOP_SRC,
    impression_id_target(
        table="activity_stream_stable.impression_stats_v1"
    ): IMPRESSION_SRC,
    impression_id_target(table="activity_stream_stable.spoc_fills_v1"): IMPRESSION_SRC,
    impression_id_target(
        table="messaging_system_stable.undesired_events_v1"
    ): IMPRESSION_SRC,
    impression_id_target(
        table="messaging_system_stable.personalization_experiment_v1"
    ): IMPRESSION_SRC,
    # sync
    DeleteTarget(table="telemetry_stable.sync_v4", field=SYNC_IDS): SYNC_SOURCES,
    DeleteTarget(table="telemetry_stable.sync_v5", field=SYNC_IDS): SYNC_SOURCES,
    # fxa
    user_id_target(
        table="firefox_accounts_derived.fxa_amplitude_export_v1"
    ): FXA_HMAC_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_amplitude_user_ids_v1"
    ): FXA_HMAC_SRC,
    fxa_user_id_target(
        table="firefox_accounts_derived.fxa_auth_bounce_events_v1"
    ): FXA_SRC,
    fxa_user_id_target(table="firefox_accounts_derived.fxa_auth_events_v1"): FXA_SRC,
    fxa_user_id_target(table="firefox_accounts_derived.fxa_content_events_v1"): FXA_SRC,
    fxa_user_id_target(table="firefox_accounts_derived.fxa_oauth_events_v1"): FXA_SRC,
    user_id_target(table="firefox_accounts_derived.fxa_users_daily_v1"): FXA_SRC,
    user_id_target(table="firefox_accounts_derived.fxa_users_last_seen_v1"): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_daily_v1"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_first_seen_v1"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_last_seen_v1"
    ): FXA_SRC,
}

SEARCH_IGNORE_TABLES = {source.table for source in SOURCES}
SEARCH_IGNORE_TABLES |= {target.table for target in DELETE_TARGETS}
# these tables have a known user identifier, but do not yet have associated
# deletion requests, or do not keep data for older than 30 days
SEARCH_IGNORE_TABLES |= {
    target.table
    for target in [
        # glean migration
        glean_target(table="org_mozilla_fenix_nightly_stable.migration_v1"),
        glean_target(table="org_mozilla_fenix_stable.migration_v1"),
        glean_target(table="org_mozilla_fennec_aurora_stable.migration_v1"),
        glean_target(table="org_mozilla_firefox_beta_stable.migration_v1"),
        glean_target(table="org_mozilla_firefox_stable.migration_v1"),
        # pocket
        DeleteTarget(table="pocket_stable.fire_tv_events_v1", field=POCKET_ID),
        # mobile
        client_id_target(table="mobile_stable.activation_v1"),
        client_id_target(table="telemetry_stable.core_v1"),
        client_id_target(table="telemetry_stable.core_v2"),
        client_id_target(table="telemetry_stable.core_v3"),
        client_id_target(table="telemetry_stable.core_v4"),
        client_id_target(table="telemetry_stable.core_v5"),
        client_id_target(table="telemetry_stable.core_v6"),
        client_id_target(table="telemetry_stable.core_v7"),
        client_id_target(table="telemetry_stable.core_v8"),
        client_id_target(table="telemetry_stable.core_v9"),
        client_id_target(table="telemetry_stable.core_v10"),
        client_id_target(table="telemetry_stable.mobile_event_v1"),
        client_id_target(table="telemetry_stable.mobile_metrics_v1"),
        # internal
        client_id_target(table="eng_workflow_stable.build_v1"),
        # other
        DeleteTarget(table="telemetry_stable.pioneer_study_v4", field=PIONEER_ID),
        DeleteTarget(
            table="telemetry_stable.pre_account_v4", field=ECOSYSTEM_CLIENT_ID
        ),
    ]
}

# these fields should be ignored by search because they are not user identifiers
SEARCH_IGNORE_FIELDS = {
    ("telemetry_derived.survey_gizmo_daily_attitudes", SHIELD_ID),
    # id is the source for document_id in these tables
    ("firefox_launcher_process_stable.launcher_process_failure_v1", ID),
    ("telemetry_derived.origin_content_blocking", ID),
    ("telemetry_stable.anonymous_v4", ID),
    ("telemetry_stable.optout_v4", ID),
    ("telemetry_stable.pre_account_v4", ID),
    ("telemetry_stable.prio_v4", ID),
}


def find_glean_targets(pool, client, project=SHARED_PROD):
    """Return a dict like DELETE_TARGETS for glean tables."""
    datasets = {dataset.dataset_id for dataset in client.list_datasets(project)}
    glean_stable_tables = [
        table
        for tables in pool.map(
            client.list_tables,
            [
                bigquery.DatasetReference(project, dataset_id)
                for dataset_id in datasets
                if dataset_id.endswith("_stable")
            ],
            chunksize=1,
        )
        for table in tables
        if table.labels.get("schema_id") == GLEAN_SCHEMA_ID
    ]
    source_doctype = "deletion_request"
    sources = {
        dataset_id: DeleteSource(qualified_table_id(table), GLEAN_CLIENT_ID, project)
        # dict comprehension will only keep the last value for a given key, so
        # sort by table_id to use the latest version
        for table in sorted(glean_stable_tables, key=lambda t: t.table_id)
        if table.table_id.startswith(source_doctype)
        # re-use source for derived tables
        for dataset_id in [
            table.dataset_id,
            re.sub("_stable$", "_derived", table.dataset_id),
        ]
        if dataset_id in datasets
    }
    return {
        **{
            # glean stable tables that have a source
            glean_target(qualified_table_id(table)): sources[table.dataset_id]
            for table in glean_stable_tables
            if table.dataset_id in sources
            and not table.table_id.startswith(source_doctype)
            # migration tables not yet supported
            and not table.table_id.startswith("migration")
        },
        **{
            # glean derived tables that contain client_id
            client_id_target(table=qualified_table_id(table)): sources[table.dataset_id]
            for table in pool.map(
                client.get_table,
                [
                    table
                    for tables in pool.map(
                        client.list_tables,
                        [
                            bigquery.DatasetReference(project, dataset_id)
                            for dataset_id in sources
                            if not dataset_id.endswith("_stable")
                        ],
                        chunksize=1,
                    )
                    for table in tables
                ],
                chunksize=1,
            )
            if any(field.name == CLIENT_ID for field in table.schema)
        },
    }


EXPERIMENT_ANALYSIS = "moz-fx-data-experiments"


def find_experiment_analysis_targets(pool, client, project=EXPERIMENT_ANALYSIS):
    """Return a dict like DELETE_TARGETS for experiment analysis tables."""
    datasets = {dataset.reference for dataset in client.list_datasets(project)}

    tables = [
        table
        for tables in pool.map(client.list_tables, datasets, chunksize=1,)
        for table in tables
        if table.table_type != "VIEW" and not table.table_id.startswith("statistics_")
    ]

    return {
        client_id_target(table=qualified_table_id(table)): DESKTOP_SRC
        for table in tables
    }
