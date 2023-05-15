"""Parsing of backfill yaml files."""

import enum
import os
from collections import OrderedDict
from datetime import date, datetime
from typing import List, Optional

import attr
import yaml
from yaml.representer import Representer

from bigquery_etl.query_scheduling.utils import is_email_or_github_identity

BACKFILL_FILE = "backfill.yaml"
DEFAULT_REASON = "Please provide a reason for the backfill and links to any related bugzilla or jira tickets"


class Literal(str):
    """Represents a YAML literal."""

    pass


def literal_presenter(dumper, data):
    """Literal representer for YAML output."""
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


yaml.add_representer(Literal, literal_presenter)
yaml.add_representer(OrderedDict, Representer.represent_dict)


class BackfillStatus(enum.Enum):
    """Represents backfill status types."""

    DRAFTING = "Drafting"
    VALIDATING = "Validating"
    COMPLETE = "Complete"


@attr.s(auto_attribs=True)
class Backfill:
    """
    Representation of a backfill entry configuration.

    Uses attrs to simplify the class definition and provide validation.
    Docs: https://www.attrs.org
    """

    entry_date: datetime = attr.ib()
    start_date: datetime = attr.ib()
    end_date: datetime = attr.ib()
    excluded_dates: Optional[List[datetime]] = attr.ib()
    reason: str = attr.ib()
    watchers: List[str] = attr.ib()
    status: BackfillStatus = attr.ib()

    @entry_date.validator
    def validate_entry_date(self, attribute, value):
        """Check that provided entry date is valid."""
        if date.today() < value:
            raise ValueError(f"Invalid entry date: {value}.")

    @start_date.validator
    def validate_start_date(self, attribute, value):
        """Check that provided start date is valid."""
        if self.end_date < value or self.entry_date < value or date.today() < value:
            raise ValueError(f"Invalid start date: {value}.")

    @end_date.validator
    def validate_end_date(self, attribute, value):
        """Check that provided end date is valid."""
        if (
            value < self.start_date
            or self.entry_date < self.end_date
            or date.today() < self.entry_date
        ):
            raise ValueError(f"Invalid end date: {value}.")

    @excluded_dates.validator
    def validate_excluded_dates(self, attribute, value):
        """Check that provided excluded dates are valid."""
        if not all(map(lambda e: self.start_date < e < self.end_date, value)):
            raise ValueError(f"Invalid excluded date: {value}.")

    @watchers.validator
    def validate_watchers(self, attribute, value):
        """Check that provided email addresses or github identities for owners are valid."""
        if not value or not all(map(lambda e: is_email_or_github_identity(e), value)):
            raise ValueError(f"Invalid email or Github identity for watchers: {value}.")

    @reason.validator
    def validate_reason(self, attribute, value):
        """Check that provided reason is not empty."""
        if not value or len(value) == 0:
            raise ValueError(f"Invalid reason: {value}.")

    # TODO: Fix error:  TypeError: hasattr(): attribute name must be string
    @status.validator
    def validate_status(self, attribute, value):
        """Check that provided status is valid."""
        if not hasattr(BackfillStatus, value.name):
            raise ValueError(f"Invalid status: {value.name}.")

    @staticmethod
    def is_backfill_file(file_path) -> bool:
        """
        Check if the provided file is a backfill file.

        Checks if the name and file format match the backfill file requirements.
        """
        return os.path.basename(file_path) == BACKFILL_FILE

    @classmethod
    def entries_from_file(cls, file):
        """
        Parse all backfill entries from the provided yaml file.

        Create a list to store all backfill entries.
        """
        if not cls.is_backfill_file(file):
            raise ValueError(f"Invalid file: {file}.")

        backfill_entries = []

        with open(file, "r") as yaml_stream:
            try:
                backfills = yaml.safe_load(yaml_stream) or []

                for entry_date, entry in backfills.items():
                    excluded_dates = []
                    if "excluded_dates" in entry:
                        excluded_dates = entry["excluded_date"]

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
                raise e

            return backfill_entries

    def to_yaml(self) -> str:
        """Create dictionary version of yaml for writing to file."""
        yaml_dict = {
            self.entry_date: {
                "start_date": self.start_date,
                "end_date": self.end_date,
                "excluded_dates": self.excluded_dates,
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
