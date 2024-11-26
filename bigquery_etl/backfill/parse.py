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
DEFAULT_BILLING_PROJECT = "moz-fx-data-backfill-slots"


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

    INITIATE = "Initiate"
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
    custom_query_path: Optional[str] = attr.ib(None)
    shredder_mitigation: Optional[bool] = attr.ib(False)
    override_retention_limit: Optional[bool] = attr.ib(False)
    billing_project: Optional[str] = attr.ib(None)

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
            custom_query_path = {self.custom_query_path}
            shredder_mitigation = {self.shredder_mitigation}
            override_retention_limit = {self.override_retention_limit}
            """

        return backfill_str.replace("'", "")

    @entry_date.validator
    def validate_entry_date(self, attribute, value):
        """Check that provided entry date is not in the future."""
        if date.today() < value:
            raise ValueError(f"Backfill entry {value} can't be in the future.")

    @start_date.validator
    def validate_start_date(self, attribute, value):
        """Check that provided start date is before end date and entry date."""
        if self.end_date < value or self.entry_date < value:
            raise ValueError(f"Invalid start date: {value}.")

    @end_date.validator
    def validate_end_date(self, attribute, value):
        """Check that provided end date is after start date and before entry date."""
        if value < self.start_date or value > self.entry_date:
            raise ValueError(f"Invalid end date: {value}.")

    @excluded_dates.validator
    def validate_excluded_dates(self, attribute, value):
        """Check that provided excluded dates are between start and end dates, are sorted and contain no duplicates."""
        if not all(map(lambda e: self.start_date < e < self.end_date, value)):
            raise ValueError(f"Invalid excluded dates: {value}.")

        if not value == sorted(value):
            raise ValueError(
                f"Existing backfill entry with excluded dates not sorted: {value}."
            )
        if not len(value) == len(set(value)):
            raise ValueError(
                f"Existing backfill entry with duplicate excluded dates: {value}."
            )

    @watchers.validator
    def validate_watchers(self, attribute, value):
        """Check that provided watchers are valid emails or Github identity with no duplicates."""
        if not value or not all(
            map(lambda e: e and is_email_or_github_identity(e), value)
        ):
            raise ValueError(f"Invalid email or Github identity for watchers: {value}.")

        if len(value) != len(set(value)):
            raise ValueError(f"Duplicate watcher in ({value}).")

    @reason.validator
    def validate_reason(self, attribute, value):
        """Check that provided status is not empty."""
        if not value:
            raise ValueError("Reason in backfill entry should not be empty.")

    @status.validator
    def validate_status(self, attribute, value):
        """Check that provided status is valid."""
        if not hasattr(BackfillStatus, value.name):
            raise ValueError(f"Invalid status: {value.name}.")

    @billing_project.validator
    def validate_billing_project(self, attribute, value):
        """Check that billing project is valid."""
        if value and not value.startswith("moz-fx-data-backfill-"):
            raise ValueError(
                f"Invalid billing project: {value}.  Please use one of the projects assigned to backfills."
            )

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
                    if status is not None and entry["status"] != status:
                        continue

                    backfill = cls(
                        entry_date=entry_date,
                        start_date=entry["start_date"],
                        end_date=entry["end_date"],
                        excluded_dates=entry.get("excluded_dates", []),
                        reason=entry["reason"],
                        watchers=entry["watchers"],
                        status=BackfillStatus[entry["status"].upper()],
                        custom_query_path=entry.get("custom_query_path", None),
                        shredder_mitigation=entry.get("shredder_mitigation", False),
                        override_retention_limit=entry.get(
                            "override_retention_limit", False
                        ),
                        billing_project=entry.get("billing_project", None),
                    )

                    backfill_entries.append(backfill)

            except yaml.YAMLError as e:
                raise ValueError(f"Unable to parse Backfill file {file}") from e
            except ValueError as e:
                raise ValueError(f"Unable to parse Backfill file {file}: {e}") from e

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
                "custom_query_path": self.custom_query_path,
                "shredder_mitigation": self.shredder_mitigation,
                "override_retention_limit": self.override_retention_limit,
                "billing_project": self.billing_project,
            }
        }

        if yaml_dict[self.entry_date]["excluded_dates"] == []:
            del yaml_dict[self.entry_date]["excluded_dates"]

        if yaml_dict[self.entry_date]["custom_query_path"] is None:
            del yaml_dict[self.entry_date]["custom_query_path"]

        if yaml_dict[self.entry_date]["shredder_mitigation"] is None:
            del yaml_dict[self.entry_date]["shredder_mitigation"]

        if yaml_dict[self.entry_date]["override_retention_limit"] is None:
            del yaml_dict[self.entry_date]["override_retention_limit"]

        if yaml_dict[self.entry_date]["billing_project"] is None:
            del yaml_dict[self.entry_date]["billing_project"]

        return yaml.dump(
            yaml_dict,
            sort_keys=False,
        )
