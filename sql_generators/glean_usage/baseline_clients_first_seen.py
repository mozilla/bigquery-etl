"""Generate and run baseline_clients_first_seen queries for Glean apps."""

from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "baseline_clients_first_seen_v1"
PREFIX = "first_seen"


class BaselineClientsFirstSeenTable(GleanTable):
    """Represents generated baseline_clients_first_seen table."""

    def __init__(self):
        """Initialize baseline_clients_first_seen table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.no_init = False
        self.custom_render_kwargs = {}

    def generate_per_app_id(
        self, project_id, baseline_table, output_dir=None, use_cloud_function=True, app_info=[]
    ):
        """Generate per-app_id datasets."""
        self.custom_render_kwargs = dict(
            # do not match on org_mozilla_firefoxreality
            fennec_id=any(
                (f"{app_id}_stable" in baseline_table)
                for app_id in [
                    "org_mozilla_firefox",
                    "org_mozilla_fenix_nightly",
                    "org_mozilla_fennec_aurora",
                    "org_mozilla_firefox_beta",
                    "org_mozilla_fenix",
                ]
            )
        )

        GleanTable.generate_per_app_id(
            self,
            project_id,
            baseline_table,
            output_dir=output_dir,
            app_info=app_info
        )
