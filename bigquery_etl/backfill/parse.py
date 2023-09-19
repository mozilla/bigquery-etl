"""Parse backfill entries."""

import enum
import os
from datetime import date
from pathlib import Path
from typing import List, Optional

import attr
import yaml

from bigquery_etl.query_scheduling.utils import is_email_or_github_identity

BACKFILL_FILE = "backfill.yaml"
DEFAULT_WATCHER = "nobody@mozilla.com"
DEFAULT_REASON = "Please provide a reason for the backfill and links to any related bugzilla or jira tickets"


class UniqueKeyLoader(yaml.SafeLoader):
    """YAML loader to check duplicate keys."""

    def construct_mapping(self, node, deep=False):
        """Create mapping while checking for duplicate keys."""
        mapping = set()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise ValueError(
                    f"Backfill entry already exists with entry date: {key}."
                )
            mapping.add(key)
        return super().construct_mapping(node, deep)


class Literal(str):
    """Represents a YAML literal."""

    pass


def literal_presenter(dumper, data):
    """Literal representer for YAML output."""
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


yaml.add_representer(Literal, literal_presenter)


class BackfillStatus(enum.Enum):
    """Represents backfill status types."""

    DRAFTING = "Drafting"
    VALIDATED = "Validated"
    COMPLETE = "Complete"


@attr.s(auto_attribs=True)
class Backfill:
    """
    Representation of a backfill entry configuration.

    Uses attrs to simplify the class definition and provide validation.
    Docs: https://www.attrs.org
    """

    entry_date: date = attr.ib()
    start_date: date = attr.ib()
    end_date: date = attr.ib()
    excluded_dates: List[date] = attr.ib()
    reason: str = attr.ib()
    watchers: List[str] = attr.ib()
    status: BackfillStatus = attr.ib()

    def __str__(self):
        """Return print friendly string of backfill object."""
        backfill_str = f"""
            entry_date = {self.entry_date}
            start_date = {self.start_date}
            end_date = {self.end_date}
            """.rstrip()

        if self.excluded_dates:
            backfill_str += f"""
            excluded_dates = {[str(e) for e in self.excluded_dates]}
            """.rstrip()

        backfill_str += f"""
            reason = {self.reason}
            watcher(s) = {self.watchers}
            status = {self.status.value}
            """

        return backfill_str.replace("'", "")

    @entry_date.validator
    def validate_entry_date(self, attribute, value):
        """Check that provided entry date is valid."""
        if date.today() < value:
            raise ValueError(f"Backfill entry {value} can't be in the future.")

    @start_date.validator
    def validate_start_date(self, attribute, value):
        """Check that provided start date is valid."""
        if self.end_date < value or self.entry_date < value:
            raise ValueError(f"Invalid start date: {value}.")

    @end_date.validator
    def validate_end_date(self, attribute, value):
        """Check that provided end date is valid."""
        if value < self.start_date or self.entry_date < self.end_date:
            raise ValueError(f"Invalid end date: {value}.")

    @excluded_dates.validator
    def validate_excluded_dates(self, attribute, value):
        """Check that provided excluded dates are valid."""
        if not all(map(lambda e: self.start_date < e < self.end_date, value)):
            raise ValueError(f"Invalid excluded dates: {value}.")

    @watchers.validator
    def validate_watchers(self, attribute, value):
        """Check that provided watchers are valid."""
        if not value or not all(
            map(lambda e: e and is_email_or_github_identity(e), value)
        ):
            raise ValueError(f"Invalid email or Github identity for watchers: {value}.")

    @status.validator
    def validate_status(self, attribute, value):
        """Check that provided status is valid."""
        if not hasattr(BackfillStatus, value.name):
            raise ValueError(f"Invalid status: {value.name}.")

    @staticmethod
    def is_backfill_file(file_path: Path) -> bool:
        """Check if the provided file is a backfill file."""
        return os.path.basename(file_path) == BACKFILL_FILE

    @classmethod
    def entries_from_file(
        cls, file: Path, status: Optional[str] = None
    ) -> List["Backfill"]:
        """
        Parse all backfill entries from the provided yaml file.

        Return a list with all backfill entries.

        @param status:  optional status param for filtering backfill entries with specific status.
        If status is not provided, all backfill entries will be returned.
        """
        if not cls.is_backfill_file(file):
            raise ValueError(f"Invalid file: {file}.")

        backfill_entries: List[Backfill] = []

        with open(file, "r") as yaml_stream:
            try:
                backfills = yaml.load(yaml_stream, Loader=UniqueKeyLoader) or {}

                for entry_date, entry in backfills.items():
                    if status is not None and entry["status"].lower() != status.lower():
                        continue

                    excluded_dates = []
                    if "excluded_dates" in entry:
                        excluded_dates = entry["excluded_dates"]

                    backfill = cls(
                        entry_date=entry_date,
                        start_date=entry["start_date"],
                        end_date=entry["end_date"],
                        excluded_dates=excluded_dates,
                        reason=entry["reason"],
                        watchers=entry["watchers"],
                        status=BackfillStatus[entry["status"].upper()],
                    )

                    backfill_entries.append(backfill)

            except yaml.YAMLError as e:
                raise ValueError(f"Unable to parse Backfill file {file}") from e

            return backfill_entries

    def to_yaml(self) -> str:
        """Create dictionary version of yaml for writing to file."""
        yaml_dict = {
            self.entry_date: {
                "start_date": self.start_date,
                "end_date": self.end_date,
                "excluded_dates": sorted(self.excluded_dates),
                "reason": self.reason,
                "watchers": self.watchers,
                "status": self.status.value,
            }
        }

        if yaml_dict[self.entry_date]["excluded_dates"] == []:
            del yaml_dict[self.entry_date]["excluded_dates"]

        return yaml.dump(
            yaml_dict,
            sort_keys=False,
        )
