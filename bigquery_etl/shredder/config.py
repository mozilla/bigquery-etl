#!/usr/bin/env python3

"""Meta data about tables and ids for self serve deletion."""

from dataclasses import dataclass
from functools import partial
from itertools import chain
from typing import Optional, Tuple

from ..util.sql_table_id import sql_table_id


SHARED_PROD = "moz-fx-data-shared-prod"


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

    @property
    def sql_table_id(self):
        """Make sql_table_id available as a property for easier templating."""
        return sql_table_id(self)


@dataclass(frozen=True)
class ClusterCondition:
    """Data class for cluster condition."""

    condition: str
    needs_clustering: bool


@dataclass(frozen=True)
class DeleteTarget:
    """Data class for deletion request target.

    Without cluster conditions rows will be removed using either one DELETE
    statement for the whole table, or one DELETE statement per partition if the
    table is larger than some configurable threshold.

    When provided cluster conditions are used to divide up deletes into parts
    smaller than partitions. This is a mitigation specifically for main_v4
    because it has thousands of sparsely populated columns and partitions in
    excess of 10TiB, resulting in very slow DELETE performance that could
    exceed 6 hours for a single partition and makes flat-rate pricing more
    expensive than on-demand pricing.

    To improve performance vs DELETE operations, cluster conditions can set
    needs_clustering to False to avoid the overhead of clustering results when
    the condition identifies a single cluster.

    Each cluster condition is used with a SELECT statement to extract rows from
    the target table into an intermediate table while filtering out rows with
    deletion requests. The intermediate tables are then combined using a copy
    operation to overwrite target table partitions.

    This means that every row must be covered by precisely one cluster
    condition. Any rows not covered by a cluster condition would be dropped,
    and any rows covered by multiple conditions would be duplicated.
    """

    table: str
    field: str
    cluster_conditions: Optional[Tuple[ClusterCondition, ...]] = None
    project: str = SHARED_PROD

    @property
    def table_id(self):
        """Table Id."""
        return self.table.split(".", 1)[-1]

    @property
    def dataset_id(self):
        """Dataset Id."""
        return self.table.split(".", 1)[0]

    @property
    def sql_table_id(self):
        """Make sql_table_id available as a property for easier templating."""
        return sql_table_id(self)


CLIENT_ID = "client_id"
GLEAN_CLIENT_ID = "client_info.client_id"
IMPRESSION_ID = "impression_id"
USER_ID = "user_id"
POCKET_ID = "pocket_id"
SHIELD_ID = "shield_id"
ECOSYSTEM_CLIENT_ID = "payload.ecosystem_client_id"
PIONEER_ID = "payload.pioneer_id"
ID = "id"

DESKTOP_SRC = DeleteSource(
    table="telemetry_stable.deletion_request_v4", field=CLIENT_ID
)
IMPRESSION_SRC = DeleteSource(
    table="telemetry_stable.deletion_request_v4",
    field="payload.scalars.parent.deletion_request_impression_id",
)
FENIX_NIGHTLY_SRC = DeleteSource(
    table="org_mozilla_fenix_nightly_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
FENIX_SRC = DeleteSource(
    table="org_mozilla_fenix_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
FENNEC_AURORA_SRC = DeleteSource(
    table="org_mozilla_fennec_aurora_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
GLEAN_FIREFOX_BETA_SRC = DeleteSource(
    table="org_mozilla_firefox_beta_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
GLEAN_FIREFOX_SRC = DeleteSource(
    table="org_mozilla_firefox_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
FOGOTYPE_SRC = DeleteSource(
    table="org_mozilla_fogotype_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
IOS_LOCKBOX_SRC = DeleteSource(
    table="org_mozilla_ios_lockbox_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
REFERENCE_BROWSER_SRC = DeleteSource(
    table="org_mozilla_reference_browser_stable.deletion_request_v1",
    field=GLEAN_CLIENT_ID,
)
TV_FIREFOX_SRC = DeleteSource(
    table="org_mozilla_tv_firefox_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
VRBROWSER_SRC = DeleteSource(
    table="org_mozilla_vrbrowser_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
)
SOURCES = [
    DESKTOP_SRC,
    IMPRESSION_SRC,
    FENIX_NIGHTLY_SRC,
    FENIX_SRC,
    FENNEC_AURORA_SRC,
    GLEAN_FIREFOX_BETA_SRC,
    GLEAN_FIREFOX_SRC,
    FOGOTYPE_SRC,
    IOS_LOCKBOX_SRC,
    REFERENCE_BROWSER_SRC,
    TV_FIREFOX_SRC,
    VRBROWSER_SRC,
]


client_id_target = partial(DeleteTarget, field=CLIENT_ID)
glean_target = partial(DeleteTarget, field=GLEAN_CLIENT_ID)
impression_id_target = partial(DeleteTarget, field=IMPRESSION_ID)

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
    client_id_target(table="telemetry_derived.clients_last_seen_v1"): DESKTOP_SRC,
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
    client_id_target(
        table="telemetry_stable.main_v4",
        cluster_conditions=tuple(
            ClusterCondition(condition, needs_clustering)
            for condition, needs_clustering in chain(
                {
                    f"sample_id = {sample_id} AND normalized_channel = 'release'": False
                    for sample_id in range(100)
                }.items(),
                [
                    (
                        "(sample_id IS NULL "
                        "OR normalized_channel IS NULL "
                        "OR normalized_channel != 'release')",
                        True,
                    )
                ],
            )
        ),
    ): DESKTOP_SRC,
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
    # glean
    client_id_target(table="org_mozilla_fenix_derived.clients_daily_v1"): FENIX_SRC,
    client_id_target(table="org_mozilla_fenix_derived.clients_last_seen_v1"): FENIX_SRC,
    glean_target(
        table="org_mozilla_fenix_nightly_stable.activation_v1"
    ): FENIX_NIGHTLY_SRC,
    glean_target(
        table="org_mozilla_fenix_nightly_stable.baseline_v1"
    ): FENIX_NIGHTLY_SRC,
    glean_target(
        table="org_mozilla_fenix_nightly_stable.bookmarks_sync_v1"
    ): FENIX_NIGHTLY_SRC,
    glean_target(table="org_mozilla_fenix_nightly_stable.events_v1"): FENIX_NIGHTLY_SRC,
    glean_target(
        table="org_mozilla_fenix_nightly_stable.history_sync_v1"
    ): FENIX_NIGHTLY_SRC,
    glean_target(
        table="org_mozilla_fenix_nightly_stable.logins_sync_v1"
    ): FENIX_NIGHTLY_SRC,
    glean_target(
        table="org_mozilla_fenix_nightly_stable.metrics_v1"
    ): FENIX_NIGHTLY_SRC,
    glean_target(table="org_mozilla_fenix_stable.activation_v1"): FENIX_SRC,
    glean_target(table="org_mozilla_fenix_stable.baseline_v1"): FENIX_SRC,
    glean_target(table="org_mozilla_fenix_stable.bookmarks_sync_v1"): FENIX_SRC,
    glean_target(table="org_mozilla_fenix_stable.events_v1"): FENIX_SRC,
    glean_target(table="org_mozilla_fenix_stable.history_sync_v1"): FENIX_SRC,
    glean_target(table="org_mozilla_fenix_stable.logins_sync_v1"): FENIX_SRC,
    glean_target(table="org_mozilla_fenix_stable.metrics_v1"): FENIX_SRC,
    glean_target(
        table="org_mozilla_reference_browser_stable.baseline_v1"
    ): REFERENCE_BROWSER_SRC,
    glean_target(
        table="org_mozilla_reference_browser_stable.events_v1"
    ): REFERENCE_BROWSER_SRC,
    glean_target(
        table="org_mozilla_reference_browser_stable.metrics_v1"
    ): REFERENCE_BROWSER_SRC,
    glean_target(table="org_mozilla_tv_firefox_stable.baseline_v1"): TV_FIREFOX_SRC,
    glean_target(table="org_mozilla_tv_firefox_stable.events_v1"): TV_FIREFOX_SRC,
    glean_target(table="org_mozilla_tv_firefox_stable.metrics_v1"): TV_FIREFOX_SRC,
    glean_target(table="org_mozilla_vrbrowser_stable.baseline_v1"): VRBROWSER_SRC,
    glean_target(table="org_mozilla_vrbrowser_stable.bookmarks_sync_v1"): VRBROWSER_SRC,
    glean_target(table="org_mozilla_vrbrowser_stable.events_v1"): VRBROWSER_SRC,
    glean_target(table="org_mozilla_vrbrowser_stable.history_sync_v1"): VRBROWSER_SRC,
    glean_target(table="org_mozilla_vrbrowser_stable.logins_sync_v1"): VRBROWSER_SRC,
    glean_target(table="org_mozilla_vrbrowser_stable.metrics_v1"): VRBROWSER_SRC,
    glean_target(table="org_mozilla_vrbrowser_stable.session_end_v1"): VRBROWSER_SRC,
    glean_target(
        table="org_mozilla_fennec_aurora_stable.activation_v1"
    ): FENNEC_AURORA_SRC,
    glean_target(
        table="org_mozilla_fennec_aurora_stable.baseline_v1"
    ): FENNEC_AURORA_SRC,
    glean_target(
        table="org_mozilla_fennec_aurora_stable.bookmarks_sync_v1"
    ): FENNEC_AURORA_SRC,
    glean_target(table="org_mozilla_fennec_aurora_stable.events_v1"): FENNEC_AURORA_SRC,
    glean_target(
        table="org_mozilla_fennec_aurora_stable.history_sync_v1"
    ): FENNEC_AURORA_SRC,
    glean_target(
        table="org_mozilla_fennec_aurora_stable.logins_sync_v1"
    ): FENNEC_AURORA_SRC,
    glean_target(
        table="org_mozilla_fennec_aurora_stable.metrics_v1"
    ): FENNEC_AURORA_SRC,
    glean_target(
        table="org_mozilla_firefox_beta_stable.activation_v1"
    ): GLEAN_FIREFOX_BETA_SRC,
    glean_target(
        table="org_mozilla_firefox_beta_stable.baseline_v1"
    ): GLEAN_FIREFOX_BETA_SRC,
    glean_target(
        table="org_mozilla_firefox_beta_stable.bookmarks_sync_v1"
    ): GLEAN_FIREFOX_BETA_SRC,
    glean_target(
        table="org_mozilla_firefox_beta_stable.events_v1"
    ): GLEAN_FIREFOX_BETA_SRC,
    glean_target(
        table="org_mozilla_firefox_beta_stable.history_sync_v1"
    ): GLEAN_FIREFOX_BETA_SRC,
    glean_target(
        table="org_mozilla_firefox_beta_stable.logins_sync_v1"
    ): GLEAN_FIREFOX_BETA_SRC,
    glean_target(
        table="org_mozilla_firefox_beta_stable.metrics_v1"
    ): GLEAN_FIREFOX_BETA_SRC,
    glean_target(table="org_mozilla_firefox_stable.activation_v1"): GLEAN_FIREFOX_SRC,
    glean_target(table="org_mozilla_firefox_stable.baseline_v1"): GLEAN_FIREFOX_SRC,
    glean_target(
        table="org_mozilla_firefox_stable.bookmarks_sync_v1"
    ): GLEAN_FIREFOX_SRC,
    glean_target(table="org_mozilla_firefox_stable.events_v1"): GLEAN_FIREFOX_SRC,
    glean_target(table="org_mozilla_firefox_stable.history_sync_v1"): GLEAN_FIREFOX_SRC,
    glean_target(table="org_mozilla_firefox_stable.logins_sync_v1"): GLEAN_FIREFOX_SRC,
    glean_target(table="org_mozilla_firefox_stable.metrics_v1"): GLEAN_FIREFOX_SRC,
    glean_target(table="org_mozilla_fogotype_stable.baseline_v1"): FOGOTYPE_SRC,
    glean_target(table="org_mozilla_fogotype_stable.events_v1"): FOGOTYPE_SRC,
    glean_target(table="org_mozilla_fogotype_stable.metrics_v1"): FOGOTYPE_SRC,
    glean_target(table="org_mozilla_fogotype_stable.prototype_v1"): FOGOTYPE_SRC,
    glean_target(table="org_mozilla_ios_lockbox_stable.baseline_v1"): IOS_LOCKBOX_SRC,
    glean_target(table="org_mozilla_ios_lockbox_stable.events_v1"): IOS_LOCKBOX_SRC,
    glean_target(table="org_mozilla_ios_lockbox_stable.metrics_v1"): IOS_LOCKBOX_SRC,
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
        # activity stream
        impression_id_target(table="activity_stream_stable.impression_stats_v1"),
        impression_id_target(table="activity_stream_stable.spoc_fills_v1"),
        impression_id_target(table="messaging_system_stable.undesired_events_v1"),
        # pocket
        DeleteTarget(table="pocket_stable.fire_tv_events_v1", field=POCKET_ID),
        # fxa
        DeleteTarget(table="fxa_users_services_daily_v1", field=USER_ID),
        DeleteTarget(table="fxa_users_services_first_seen_v1", field=USER_ID),
        DeleteTarget(table="fxa_users_services_last_seen_v1", field=USER_ID),
        DeleteTarget(
            table="telemetry_derived.devtools_events_amplitude_v1", field=USER_ID
        ),
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
        client_id_target(table="activity_stream_stable.events_v1"),
        client_id_target(table="eng_workflow_stable.build_v1"),
        client_id_target(table="messaging_system_stable.cfr_v1"),
        client_id_target(table="messaging_system_stable.onboarding_v1"),
        client_id_target(table="messaging_system_stable.snippets_v1"),
        # other
        DeleteTarget(table="telemetry_stable.pioneer_study_v4", field=PIONEER_ID),
        DeleteTarget(
            table="telemetry_stable.pre_account_v4", field=ECOSYSTEM_CLIENT_ID
        ),
        # glam
        client_id_target(table="telemetry_derived.clients_histogram_aggregates_v1"),
        client_id_target(table="telemetry_derived.clients_scalar_aggregates_v1"),
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
    ("telemetry_stable.sync_v4", ID),
    ("telemetry_stable.sync_v5", ID),
}
