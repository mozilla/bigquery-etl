#!/usr/bin/env python3

"""Meta data about tables and ids for self serve deletion."""

import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from itertools import chain
from multiprocessing.pool import ThreadPool
from typing import Dict, List

import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..util.bigquery_id import qualified_table_id

SHARED_PROD = "moz-fx-data-shared-prod"
GLEAN_SCHEMA_ID = "glean_ping_1"
GLEAN_MIN_SCHEMA_ID = "glean-min_ping_1"
GLEAN_APP_LISTINGS_URL = "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"


@dataclass(frozen=True)
class DeleteSource:
    """Data class for deletion request source."""

    table: str
    field: str
    project: str = SHARED_PROD
    conditions: tuple[str, ...] = ()

    def __post_init__(self):
        """Validate the table string."""
        if len(self.table.split(".")) != 2:
            raise ValueError(
                "DeleteSource table must be in the 'dataset.table' format."
            )

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
    field: str | tuple[str, ...]
    project: str = SHARED_PROD

    def __post_init__(self):
        """Validate the table string."""
        if len(self.table.split(".")) != 2:
            raise ValueError(
                "DeleteTarget table must be in the 'dataset.table' format."
            )

    @property
    def table_id(self):
        """Table Id."""
        return self.table.partition(".")[2]

    @property
    def dataset_id(self):
        """Dataset Id."""
        return self.table.partition(".")[0]

    @property
    def fields(self) -> tuple[str, ...]:
        """Fields."""
        if isinstance(self.field, tuple):
            return self.field
        return (self.field,)


DeleteIndex = dict[DeleteTarget, DeleteSource | tuple[DeleteSource, ...]]

CLIENT_ID = "client_id"
GLEAN_CLIENT_ID = "client_info.client_id"
GLEAN_USAGE_PROFILE_ID = "metrics.uuid.usage_profile_id"
IMPRESSION_ID = "impression_id"
USER_ID = "user_id"
POCKET_ID = "pocket_id"
SHIELD_ID = "shield_id"
PIONEER_ID = "pioneer_id"
RALLY_ID = "metrics.uuid.rally_id"
RALLY_ID_TOP_LEVEL = "rally_id"
ID = "id"
FXA_USER_ID = "jsonPayload.fields.user_id"
# these must be in the same order as SYNC_SOURCES
SYNC_IDS = ("SUBSTR(payload.device_id, 0, 32)", "payload.uid")
CONTEXT_ID = "context_id"
USER_CHARACTERISTICS_ID = "metrics.uuid.characteristics_client_identifier"

DESKTOP_SRC = DeleteSource(
    table="telemetry_stable.deletion_request_v4", field=CLIENT_ID
)
DESKTOP_GLEAN_SRC = DeleteSource(
    table="firefox_desktop_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
IMPRESSION_SRC = DeleteSource(
    table="telemetry_stable.deletion_request_v4",
    field="payload.scalars.parent.deletion_request_impression_id",
)
CONTEXTUAL_SERVICES_SRC = DeleteSource(
    table="telemetry_stable.deletion_request_v4",
    field="payload.scalars.parent.deletion_request_context_id",
)
FXA_HMAC_SRC = DeleteSource(
    table="firefox_accounts.fxa_delete_events", field="hmac_user_id"
)
FXA_SRC = DeleteSource(table="firefox_accounts.fxa_delete_events", field=USER_ID)
FXA_UNHASHED_SRC = DeleteSource(
    table="firefox_accounts.fxa_delete_events", field="user_id_unhashed"
)
FXA_FRONTEND_GLEAN_SRC = DeleteSource(
    table="accounts_frontend_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
REGRETS_SRC = DeleteSource(
    table="regrets_reporter_stable.regrets_reporter_update_v1",
    field="data_deletion_request.extension_installation_uuid",
    conditions=("data_deletion_request IS NOT NULL",),
)
# these must be in the same order as SYNC_IDS
SYNC_SOURCES = (
    DeleteSource(
        table="telemetry_stable.deletion_request_v4",
        field="payload.scalars.parent.deletion_request_sync_device_id",
    ),
    DeleteSource(
        table="firefox_accounts.fxa_delete_events",
        field="SUBSTR(hmac_user_id, 0, 32)",
    ),
)
LEGACY_MOBILE_SOURCES = tuple(
    DeleteSource(
        table=f"{product}_stable.deletion_request_v1",
        field="metrics.uuid.legacy_ids_client_id",
    )
    for product in (
        "org_mozilla_ios_fennec",
        "org_mozilla_ios_firefox",
        "org_mozilla_ios_firefoxbeta",
        "org_mozilla_tv_firefox",
        "mozilla_lockbox",
    )
)
USER_CHARACTERISTICS_SRC = DeleteSource(
    table="firefox_desktop_stable.deletion_request_v1",
    field=USER_CHARACTERISTICS_ID,
)
SOURCES = (
    [
        DESKTOP_SRC,
        IMPRESSION_SRC,
        CONTEXTUAL_SERVICES_SRC,
        FXA_HMAC_SRC,
        FXA_SRC,
    ]
    + list(SYNC_SOURCES)
    + list(LEGACY_MOBILE_SOURCES)
)

LEGACY_MOBILE_IDS = tuple(CLIENT_ID for _ in LEGACY_MOBILE_SOURCES)


client_id_target = partial(DeleteTarget, field=CLIENT_ID)
glean_target = partial(DeleteTarget, field=GLEAN_CLIENT_ID)
impression_id_target = partial(DeleteTarget, field=IMPRESSION_ID)
fxa_user_id_target = partial(DeleteTarget, field=FXA_USER_ID)
user_id_target = partial(DeleteTarget, field=USER_ID)
context_id_target = partial(DeleteTarget, field=CONTEXT_ID)

DELETE_TARGETS: DeleteIndex = {
    # Other
    client_id_target(table="search_derived.acer_cohort_v1"): DESKTOP_SRC,
    client_id_target(
        table="search_derived.mobile_search_clients_daily_v1"
    ): DESKTOP_SRC,
    client_id_target(
        table="search_derived.mobile_search_clients_daily_v2"
    ): DESKTOP_SRC,
    client_id_target(table="search_derived.search_clients_daily_v8"): DESKTOP_SRC,
    client_id_target(
        table="telemetry_derived.desktop_engagement_clients_v1"
    ): DESKTOP_SRC,
    client_id_target(table="search_derived.search_clients_last_seen_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.clients_daily_v6"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.clients_daily_joined_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.clients_last_seen_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.clients_last_seen_v2"): DESKTOP_SRC,
    client_id_target(
        table="telemetry_derived.clients_last_seen_joined_v1"
    ): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.core_clients_daily_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.core_clients_last_seen_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.event_events_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.main_events_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.main_1pct_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.main_remainder_1pct_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_derived.main_use_counter_1pct_v1"): DESKTOP_SRC,
    client_id_target(
        table="telemetry_derived.desktop_retention_clients_v1"
    ): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.block_autoplay_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.crash_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.downgrade_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.event_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.first_shutdown_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.first_shutdown_v5"): DESKTOP_SRC,
    client_id_target(
        table="telemetry_stable.first_shutdown_use_counter_v4"
    ): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.focus_event_v1"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.frecency_update_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.health_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.heartbeat_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.main_v5"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.main_use_counter_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.modules_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.new_profile_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.saved_session_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.saved_session_v5"): DESKTOP_SRC,
    client_id_target(
        table="telemetry_stable.saved_session_use_counter_v4"
    ): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.shield_icq_v1_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.shield_study_addon_v3"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.shield_study_error_v3"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.shield_study_v3"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.testpilot_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.third_party_modules_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.untrusted_modules_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.update_v4"): DESKTOP_SRC,
    client_id_target(table="telemetry_stable.voice_v4"): DESKTOP_SRC,
    DeleteTarget(
        table="telemetry_derived.mobile_engagement_clients_v1",
        field=(CLIENT_ID, CLIENT_ID),
    ): (
        DeleteSource(table="firefox_ios.deletion_request", field=GLEAN_CLIENT_ID),
        DeleteSource(table="fenix.deletion_request", field=GLEAN_CLIENT_ID),
    ),
    DeleteTarget(
        table="telemetry_derived.fx_accounts_active_daily_clients_v1",
        field=("legacy_telemetry_client_id",),
    ): (DESKTOP_SRC),
    DeleteTarget(
        table="telemetry_derived.rolling_cohorts_v2",
        field=(
            CLIENT_ID,
            CLIENT_ID,
            CLIENT_ID,
            CLIENT_ID,
            CLIENT_ID,
            CLIENT_ID,
            CLIENT_ID,
        ),
    ): (
        DESKTOP_SRC,
        DeleteSource(table="focus_android.deletion_request", field=GLEAN_CLIENT_ID),
        DeleteSource(table="firefox_ios.deletion_request", field=GLEAN_CLIENT_ID),
        DeleteSource(table="fenix.deletion_request", field=GLEAN_CLIENT_ID),
        DeleteSource(table="klar_ios.deletion_request", field=GLEAN_CLIENT_ID),
        DeleteSource(table="focus_ios.deletion_request", field=GLEAN_CLIENT_ID),
        DeleteSource(table="klar_android.deletion_request", field=GLEAN_CLIENT_ID),
    ),
    # activity stream
    DeleteTarget(
        table="messaging_system_stable.cfr_v1", field=(CLIENT_ID, IMPRESSION_ID)
    ): (DESKTOP_SRC, IMPRESSION_SRC),
    DeleteTarget(
        table="messaging_system_derived.cfr_users_daily_v1",
        field=(CLIENT_ID, IMPRESSION_ID),
    ): (DESKTOP_SRC, IMPRESSION_SRC),
    DeleteTarget(
        table="messaging_system_derived.cfr_users_last_seen_v1",
        field=(CLIENT_ID, IMPRESSION_ID),
    ): (DESKTOP_SRC, IMPRESSION_SRC),
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
    fxa_user_id_target(
        table="firefox_accounts_derived.fxa_gcp_stderr_events_v1"
    ): FXA_SRC,
    fxa_user_id_target(
        table="firefox_accounts_derived.fxa_gcp_stdout_events_v1"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_log_device_command_events_v1"
    ): FXA_HMAC_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_log_device_command_events_v2"
    ): FXA_HMAC_SRC,
    fxa_user_id_target(table="firefox_accounts_derived.fxa_oauth_events_v1"): FXA_SRC,
    fxa_user_id_target(table="firefox_accounts_derived.fxa_stdout_events_v1"): FXA_SRC,
    user_id_target(table="firefox_accounts_derived.fxa_users_daily_v1"): FXA_SRC,
    user_id_target(table="firefox_accounts_derived.fxa_users_daily_v2"): FXA_SRC,
    user_id_target(table="firefox_accounts_derived.fxa_users_first_seen_v1"): FXA_SRC,
    user_id_target(table="firefox_accounts_derived.fxa_users_first_seen_v2"): FXA_SRC,
    user_id_target(table="firefox_accounts_derived.fxa_users_last_seen_v1"): FXA_SRC,
    user_id_target(table="firefox_accounts_derived.fxa_users_last_seen_v2"): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_daily_v1"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_daily_v2"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_first_seen_v1"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_first_seen_v2"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_last_seen_v1"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_last_seen_v2"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_devices_daily_v1"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_devices_first_seen_v1"
    ): FXA_SRC,
    user_id_target(
        table="firefox_accounts_derived.fxa_users_services_devices_last_seen_v1"
    ): FXA_SRC,
    context_id_target(
        table="contextual_services_stable.topsites_click_v1"
    ): CONTEXTUAL_SERVICES_SRC,
    context_id_target(
        table="contextual_services_stable.topsites_impression_v1"
    ): CONTEXTUAL_SERVICES_SRC,
    context_id_target(
        table="contextual_services_stable.quicksuggest_click_v1"
    ): CONTEXTUAL_SERVICES_SRC,
    context_id_target(
        table="contextual_services_stable.quicksuggest_impression_v1"
    ): CONTEXTUAL_SERVICES_SRC,
    # client association ping
    DeleteTarget(
        table="firefox_desktop_stable.fx_accounts_v1",
        field=("metrics.string.client_association_uid", GLEAN_CLIENT_ID),
    ): (FXA_UNHASHED_SRC, DESKTOP_GLEAN_SRC),
    # FxA on Glean
    DeleteTarget(
        table="accounts_backend_stable.events_v1",
        field="metrics.string.account_user_id_sha256",
    ): FXA_SRC,
    DeleteTarget(
        table="accounts_backend_stable.accounts_events_v1",
        field="metrics.string.account_user_id_sha256",
    ): FXA_SRC,
    DeleteTarget(
        table="accounts_backend_derived.events_stream_v1",
        field="metrics.string.account_user_id_sha256",
    ): FXA_SRC,
    DeleteTarget(
        table="accounts_backend_derived.users_services_daily_v1",
        field="user_id_sha256",
    ): FXA_SRC,
    DeleteTarget(
        table="accounts_backend_derived.users_services_last_seen_v1",
        field="user_id_sha256",
    ): FXA_SRC,
    DeleteTarget(
        table="accounts_frontend_stable.events_v1",
        field=("metrics.string.account_user_id_sha256", GLEAN_CLIENT_ID),
    ): (FXA_SRC, FXA_FRONTEND_GLEAN_SRC),
    DeleteTarget(
        table="accounts_frontend_stable.accounts_events_v1",
        field=("metrics.string.account_user_id_sha256", GLEAN_CLIENT_ID),
    ): (FXA_SRC, FXA_FRONTEND_GLEAN_SRC),
    DeleteTarget(
        table="accounts_frontend_derived.events_stream_v1",
        field=("metrics.string.account_user_id_sha256", CLIENT_ID),
    ): (FXA_SRC, FXA_FRONTEND_GLEAN_SRC),
    # legacy mobile
    DeleteTarget(
        table="telemetry_stable.core_v1",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table="telemetry_stable.core_v2",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table="telemetry_stable.core_v3",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table="telemetry_stable.core_v4",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table="telemetry_stable.core_v5",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table="telemetry_stable.core_v6",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table="telemetry_stable.core_v7",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table="telemetry_stable.core_v8",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table="telemetry_stable.core_v9",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table="telemetry_stable.core_v10",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table="telemetry_stable.mobile_event_v1",
        field=LEGACY_MOBILE_IDS,
    ): LEGACY_MOBILE_SOURCES,
    DeleteTarget(
        table=REGRETS_SRC.table,
        field="event_metadata.extension_installation_uuid",
    ): REGRETS_SRC,
    DeleteTarget(
        table="firefox_desktop_stable.user_characteristics_v1",
        field=USER_CHARACTERISTICS_ID,
    ): USER_CHARACTERISTICS_SRC,
    # tables in Glean derived datasets that use different sources than the find_glean_targets defaults
    client_id_target(table="firefox_desktop_derived.adclick_history_v1"): DESKTOP_SRC,
    client_id_target(table="firefox_desktop_derived.client_ltv_v1"): DESKTOP_SRC,
    client_id_target(table="firefox_desktop_derived.ltv_states_v1"): DESKTOP_SRC,
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
    ]
}

# these fields should be ignored by search because they are not user identifiers
SEARCH_IGNORE_FIELDS = {
    # id is the source for document_id in these tables
    ("firefox_launcher_process_stable.launcher_process_failure_v1", ID),
    ("telemetry_stable.anonymous_v4", ID),
    ("telemetry_stable.optout_v4", ID),
    ("telemetry_stable.pre_account_v4", ID),
    ("telemetry_stable.prio_v4", ID),
}

# list of dataset_id.table_id to ignore in find_glean_targets function
GLEAN_IGNORE_LIST = {
    # deletion request table
    "firefox_desktop_derived.migration_esr_incorrect_deletion_request_v1",
    # subset of firefox_desktop_stable.pageload_v1 which doesn't have client ids
    "firefox_desktop_derived.pageload_1pct_v1",
    "firefox_desktop_derived.pageload_nightly_v1",
}


def find_glean_targets(
    pool: ThreadPool, client: bigquery.Client, project: str = SHARED_PROD
) -> DeleteIndex:
    """Return a dict like DELETE_TARGETS for glean tables.

    Note that dict values *must* be either DeleteSource or tuple[DeleteSource, ...],
    and other iterable types, e.g. list[DeleteSource] are not allowed or supported.
    """
    datasets = {dataset.dataset_id for dataset in client.list_datasets(project)}

    def stable_tables_by_schema(schema_id):
        return [
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
            if table.labels.get("schema_id") == schema_id
        ]

    glean_stable_tables = stable_tables_by_schema(GLEAN_SCHEMA_ID)

    channel_to_app_name = get_glean_channel_to_app_name_mapping()

    # create mapping of dataset -> (tables containing associated deletion requests)
    # construct values as tuples because that is what they must be in the return type
    sources: dict[str, tuple[DeleteSource, ...]] = defaultdict(tuple)
    app_names = set()
    source_doctype = "deletion_request"
    for table in glean_stable_tables:
        if table.table_id.startswith(source_doctype):
            source = DeleteSource(qualified_table_id(table), GLEAN_CLIENT_ID, project)

            channel_name = re.sub("_stable$", "", table.dataset_id)
            derived_dataset = channel_name + "_derived"
            app_name = channel_to_app_name.get(channel_name)

            # append to tuple to use every version of deletion request tables
            sources[table.dataset_id] += (source,)
            sources[derived_dataset] += (source,)

            # find the name of all apps that have a dataset of combined channels
            if app_name is not None and app_name != channel_name:
                app_names.add(app_name)
                sources[app_name + "_derived"] += (source,)

    # Use deletion request view containing all channels if found, otherwise use per-channel
    # tables as delete sources.  Some apps don't have views generated by glean_usage
    # because they are skipped in bqetl_project.yaml
    for app_name in app_names:
        try:
            source_view = client.get_table(f"{project}.{app_name}.{source_doctype}")
        except NotFound:
            pass
        else:
            source = DeleteSource(
                qualified_table_id(source_view), GLEAN_CLIENT_ID, project
            )
            sources[app_name + "_derived"] = (source,)

    glean_derived_tables = [
        table
        for table in pool.map(
            client.get_table,
            chain(
                *pool.starmap(
                    _list_tables,
                    [
                        (bigquery.DatasetReference(project, dataset_id), client)
                        for dataset_id in sources
                        if dataset_id.endswith("_derived")
                    ],
                    chunksize=1,
                )
            ),
            chunksize=1,
        )
        if table.table_type == "TABLE"
    ]

    # handle additional source for deletion requests for things like
    # https://bugzilla.mozilla.org/show_bug.cgi?id=1810236
    # table must contain client_id at the top level and be partitioned on
    # submission_timestamp
    derived_source_prefix = "additional_deletion_requests"
    for table in glean_derived_tables:
        if table.table_id.startswith(derived_source_prefix):
            source = DeleteSource(qualified_table_id(table), CLIENT_ID, project)
            channel_name = re.sub("_derived$", "", table.dataset_id)
            app_name = channel_to_app_name.get(channel_name)

            sources[channel_name + "_stable"] += (source,)
            sources[table.dataset_id] += (source,)

            if app_name is not None and app_name != channel_name:
                sources[app_name + "_derived"] += (source,)

    # skip tables already added to DELETE_TARGETS
    manually_added_tables = {target.table for target in DELETE_TARGETS.keys()}
    skipped_tables = manually_added_tables | GLEAN_IGNORE_LIST

    # Handle custom deletion requests for usage_reporting pings
    glean_min_stable_tables = stable_tables_by_schema(GLEAN_MIN_SCHEMA_ID)
    usage_reporting_sources: dict[str, tuple[DeleteSource, ...]] = defaultdict(tuple)
    for table in glean_min_stable_tables:
        if table.table_id == "usage_deletion_request_v1":
            # usage_deletion_request ping is defined in application code,
            # so we need to confirm that it has `usage_profile_id` metric
            table = client.get_table(table)
            if any(
                field.name == "metrics"
                and any(
                    metric_type_field.name == "uuid"
                    and any(
                        [
                            metric_field.name == "usage_profile_id"
                            for metric_field in metric_type_field.fields
                        ]
                    )
                    for metric_type_field in field.fields
                )
                for field in table.schema
            ):
                source = DeleteSource(
                    qualified_table_id(table), GLEAN_USAGE_PROFILE_ID, project
                )
                usage_reporting_sources[table.dataset_id] += (source,)

    return {
        **{
            # glean stable tables that have a source
            DeleteTarget(
                table=qualified_table_id(table),
                # field must be repeated for each deletion source
                field=(GLEAN_CLIENT_ID,) * len(sources[table.dataset_id]),
            ): sources[table.dataset_id]
            for table in glean_stable_tables
            if table.dataset_id in sources
            and not table.table_id.startswith(source_doctype)
            # migration tables not yet supported
            and not table.table_id.startswith("migration")
            # skip tables with explicitly excluded client ids
            and table.labels.get("include_client_id", "true").lower() != "false"
            and qualified_table_id(table) not in skipped_tables
        },
        **{
            # glean derived tables that contain client_id
            DeleteTarget(
                table=qualified_table_id(table),
                # field must be repeated for each deletion source
                field=(CLIENT_ID,) * len(sources[table.dataset_id]),
            ): sources[table.dataset_id]
            for table in glean_derived_tables
            if any(field.name == CLIENT_ID for field in table.schema)
            and not table.table_id.startswith(derived_source_prefix)
            and qualified_table_id(table) not in skipped_tables
        },
        **{
            # glean derived tables that contain client_info.client_id but not client_id
            DeleteTarget(
                table=qualified_table_id(table),
                # field must be repeated for each deletion source
                field=(GLEAN_CLIENT_ID,) * len(sources[table.dataset_id]),
            ): sources[table.dataset_id]
            for table in glean_derived_tables
            if any(
                field.name == "client_info"
                and any(
                    [nested_field.name == "client_id" for nested_field in field.fields]
                )
                for field in table.schema
            )
            and all(field.name != CLIENT_ID for field in table.schema)
            and not table.table_id.startswith(derived_source_prefix)
            and qualified_table_id(table) not in skipped_tables
        },
        **{
            # usage_reporting tables via custom usage_deletion_request ping
            DeleteTarget(
                table=qualified_table_id(table),
                # field must be repeated for each deletion source
                field=(GLEAN_USAGE_PROFILE_ID,)
                * len(usage_reporting_sources[table.dataset_id]),
            ): usage_reporting_sources[table.dataset_id]
            for table in glean_min_stable_tables
            if table.table_id == "usage_reporting_v1"
            and table.dataset_id in usage_reporting_sources
        },
    }


def get_glean_channel_to_app_name_mapping() -> Dict[str, str]:
    """Return a dict where key is the channel app id and the value is the shared app name.

    e.g. {
        "org_mozilla_firefox": "fenix",
        "org_mozilla_firefox_beta": "fenix",
        "org_mozilla_ios_firefox": "firefox_ios",
        "org_mozilla_ios_firefoxbeta": "firefox_ios",
    }
    """
    response = requests.get(GLEAN_APP_LISTINGS_URL)
    response.raise_for_status()

    app_listings = response.json()

    return {
        app["bq_dataset_family"]: app["app_name"]
        for app in app_listings
        if "bq_dataset_family" in app and "app_name" in app
    }


def _list_tables(
    dataset_ref: bigquery.DatasetReference,
    client: bigquery.Client,
) -> List:
    """Wrap bigquery list_tables and return an empty list for non-existent datasets.

    Intended to be used with thread pool map function. Some glean apps do not have
    derived datasets so this wrapper handles exceptions when listing them.
    """
    try:
        return list(client.list_tables(dataset_ref))
    except NotFound:
        return []


EXPERIMENT_ANALYSIS = "moz-fx-data-experiments"


def find_experiment_analysis_targets(
    client: bigquery.Client, project: str = EXPERIMENT_ANALYSIS
) -> DeleteIndex:
    """Return a dict like DELETE_TARGETS for experiment analysis tables.

    Note that dict values *must* be either DeleteSource or tuple[DeleteSource, ...],
    and other iterable types, e.g. list[DeleteSource] are not allowed or supported.
    """
    table_query = f"""
    SELECT
      table_schema AS dataset_id,
      table_name,
    FROM
      `{project}.region-us.INFORMATION_SCHEMA.TABLES`
    WHERE
      table_type = 'BASE TABLE'
      AND NOT STARTS_WITH(table_name, "statistics")
      AND ddl LIKE '%client_id STRING,%'
    """

    target_tables = client.query_and_wait(query=table_query)

    return {
        client_id_target(
            table=f"{table.dataset_id}.{table.table_name}", project=project
        ): DESKTOP_SRC
        for table in target_tables
    }


PIONEER_PROD = "moz-fx-data-pioneer-prod"


def find_pioneer_targets(
    pool: ThreadPool,
    client: bigquery.Client,
    project: str = PIONEER_PROD,
    study_projects: list[str] = [],
) -> DeleteIndex:
    """Return a dict like DELETE_TARGETS for Pioneer tables.

    Note that dict values *must* be either DeleteSource or tuple[DeleteSource, ...],
    and other iterable types, e.g. list[DeleteSource] are not allowed or supported.
    """

    def _has_nested_rally_id(field):
        """Check if any of the fields contains nested `metrics.uuid_rally_id`."""
        if field.name == "metrics" and field.field_type == "RECORD":
            uuid_field = next(filter(lambda f: f.name == "uuid", field.fields), None)
            if uuid_field and uuid_field.field_type == "RECORD":
                return any(field.name == "rally_id" for field in uuid_field.fields)
        return False

    def _get_tables_with_pioneer_id(dataset):
        tables_with_pioneer_id = []
        for table in client.list_tables(dataset):
            table_ref = client.get_table(table)
            if (
                any(field.name == PIONEER_ID for field in table_ref.schema)
                or any(field.name == RALLY_ID_TOP_LEVEL for field in table_ref.schema)
                or any(_has_nested_rally_id(field) for field in table_ref.schema)
            ) and table_ref.table_type != "VIEW":
                tables_with_pioneer_id.append(table_ref)
        return tables_with_pioneer_id

    def _get_client_id_field(table, deletion_request_view=False, study_name=None):
        """Determine which column should be used as client id for a given table."""
        if table.dataset_id.startswith("rally_"):
            # `rally_zero_one` is a special case where top-level rally_id is used
            # both in the ping tables and the deletion_requests view
            if table.dataset_id in ["rally_zero_one_stable", "rally_zero_one_derived"]:
                return RALLY_ID_TOP_LEVEL
            # deletion request views expose rally_id as a top-level field
            if deletion_request_view:
                return RALLY_ID_TOP_LEVEL
            else:
                return RALLY_ID
        elif table.dataset_id == "analysis":
            # Rally analysis tables do not have schemas specified upfront,
            # analysts might decide to use either nested or top-level rally_id.
            # Shared datasets, like attention stream, may also have derived
            # datasets with rally IDs
            # See https://github.com/mozilla-services/cloudops-infra/blob/master/projects/data-pioneer/tf/prod/envs/prod/study-projects/main.tf#L60-L67 # noqa
            if any(_has_nested_rally_id(field) for field in table.schema):
                return RALLY_ID
            elif any(field.name == RALLY_ID_TOP_LEVEL for field in table.schema):
                return RALLY_ID_TOP_LEVEL
            # Pioneer derived tables will have a PIONEER_ID
            elif any(field.name == PIONEER_ID for field in table.schema):
                return PIONEER_ID
            else:
                logging.error(f"Failed to find client_id field for {table}")
        else:
            return PIONEER_ID

    datasets = {
        dataset.reference
        for dataset in client.list_datasets(project)
        if dataset.reference.dataset_id.startswith("pioneer_")
        or dataset.reference.dataset_id.startswith("rally_")
    }
    # There should be a single stable and derived dataset per study
    stable_datasets = {dr for dr in datasets if dr.dataset_id.endswith("_stable")}
    derived_datasets = {dr for dr in datasets if dr.dataset_id.endswith("_derived")}

    stable_tables = [
        table
        for tables in pool.map(client.list_tables, stable_datasets, chunksize=1)
        for table in tables
    ]

    # Each derived deletion request view is a union of:
    # * corresponding deletion_request table from the _stable dataset
    # * uninstall_deletion pings from pioneer_core_stable dataset
    derived_deletion_request_views = [
        table
        for tables in pool.map(client.list_tables, derived_datasets, chunksize=1)
        for table in tables
        if (table.table_type == "VIEW" and table.table_id == "deletion_requests")
    ]

    # There is a derived dataset for each stable one
    # For simplicity when accessing this map later on, keys are changed to `_stable` here
    sources = {
        table.dataset_id.replace("_derived", "_stable"): DeleteSource(
            qualified_table_id(table),
            _get_client_id_field(table, deletion_request_view=True),
            project,
        )
        # dict comprehension will only keep the last value for a given key, so
        # sort by table_id to use the latest version
        for table in sorted(derived_deletion_request_views, key=lambda t: t.table_id)
    }

    # Dictionary mapping analysis dataset names to corresponding study names.
    # We expect analysis tables to be created only under `analysis` datasets
    # in study projects. These datasets are labeled with study names which
    # we use for discovering corresponding delete request tables later on.
    analysis_datasets = {}
    for project in study_projects:
        analysis_dataset = bigquery.DatasetReference(project, "analysis")
        labels = client.get_dataset(analysis_dataset).labels
        # study names in labels are not normalized (contain '-', not '_')
        study_name = labels.get("study_name")
        if study_name is None:
            logging.error(
                f"Dataset {analysis_dataset} does not have `study_name` label, skipping..."
            )
        else:
            analysis_datasets[analysis_dataset] = study_name

    return {
        **{
            # stable tables
            DeleteTarget(
                table=qualified_table_id(table),
                field=_get_client_id_field(table),
                project=PIONEER_PROD,
            ): sources[table.dataset_id]
            for table in stable_tables
            if not table.table_id.startswith("deletion_request_")
            and not table.table_id.startswith("pioneer_enrollment_")
            and not table.table_id.startswith("enrollment_")
            and not table.table_id.startswith("study_enrollment_")
            and not table.table_id.startswith("study_unenrollment_")
            and not table.table_id.startswith("unenrollment_")
        },
        **{
            # derived tables with pioneer_id
            DeleteTarget(
                table=qualified_table_id(table),
                field=_get_client_id_field(table),
                project=PIONEER_PROD,
            ): sources[table.dataset_id]
            for dataset in derived_datasets
            for table in _get_tables_with_pioneer_id(dataset)
        },
        **{
            # tables with pioneer_id located in study analysis projects
            DeleteTarget(
                table=qualified_table_id(table),
                field=_get_client_id_field(table, study_name=study),
                project=table.project,
            ): sources[study.replace("-", "_") + "_stable"]
            for dataset, study in analysis_datasets.items()
            for table in _get_tables_with_pioneer_id(dataset)
        },
    }
