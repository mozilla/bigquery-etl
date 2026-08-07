"""Settings shared across the column classification package."""

import re
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PROJECT = "moz-fx-data-shared-prod"
DEFAULT_DATASET = "data_governance_metadata_derived"

# The three classification_* tables are interim copies of the upstream metadata
# tables. The upstream profiler truncates its whole date partition, so writing
# our datasets into the shared tables would erase theirs. The copies go away
# once upstream supports additive writes.
PROFILES_TABLE = "classification_column_profiles_v1"
LINEAGE_TABLE = "classification_lineage_mapping_v1"
PROBES_TABLE = "classification_probe_definitions_v1"
DESTINATION_TABLE = "column_classifications_v1"

DEFAULT_MODEL = "gemini-3.5-flash-lite"
DEFAULT_VERTEX_LOCATION = "global"

TAXONOMY_PATH = Path(__file__).parent / "taxonomy_v1.json"

_BQ_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]*$")


def validate_bq_identifier(value: str, name: str) -> str:
    """Reject a project or dataset name that cannot stand in a quoted path.

    Both this config's own project and dataset and the source ones a reader is
    given reach a FROM clause as text rather than as parameters. BigQuery reports
    a malformed one legibly enough on its own; failing here names the argument at
    fault without issuing a job.
    """
    if not _BQ_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Invalid BigQuery identifier for {name}: {value!r}. "
            "Must match [A-Za-z0-9][A-Za-z0-9_-]*."
        )
    return value


@dataclass(frozen=True)
class ClassificationConfig:
    """Configuration for a single classification run."""

    run_id: str
    project: str = DEFAULT_PROJECT
    dataset: str = DEFAULT_DATASET
    model: str = DEFAULT_MODEL
    vertex_location: str = DEFAULT_VERTEX_LOCATION
    sanitize: bool = True
    # None means follow `project`. Set one of these only to call Vertex or DLP in
    # a project other than the one holding the tables, which is what a sandbox
    # run needs: neither API is enabled in the sandbox projects.
    vertex_project: str | None = None
    dlp_project: str | None = None
    # Overrides the credentials' own quota project. Leave unset in production so
    # DLP quota follows the request's parent project.
    dlp_quota_project: str | None = None

    def __post_init__(self) -> None:
        """Reject a missing run id, a non-Vertex model, or an unusable table path."""
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if not self.model.startswith("gemini-"):
            raise ValueError(
                f"model must be a Vertex-hosted Gemini model, got {self.model!r}"
            )
        # Both are interpolated into backtick-quoted table references by the
        # properties below, so they are checked here rather than at each read.
        validate_bq_identifier(self.project, "project")
        validate_bq_identifier(self.dataset, "dataset")

    @property
    def profiles_table(self) -> str:
        """Fully qualified reference to the column profiles table."""
        return f"{self.project}.{self.dataset}.{PROFILES_TABLE}"

    @property
    def lineage_table(self) -> str:
        """Fully qualified reference to the lineage mapping table."""
        return f"{self.project}.{self.dataset}.{LINEAGE_TABLE}"

    @property
    def probes_table(self) -> str:
        """Fully qualified reference to the probe definitions table."""
        return f"{self.project}.{self.dataset}.{PROBES_TABLE}"

    @property
    def destination_table(self) -> str:
        """Fully qualified reference to the classification output table."""
        return f"{self.project}.{self.dataset}.{DESTINATION_TABLE}"
