"""Tools for working with date ranges related to running Backfills."""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

from dateutil.rrule import MONTHLY, rrule

from ..metadata.parse_metadata import PartitionType

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@dataclass
class BackfillDateRange:
    """An iterator for dates in a range for backfills."""

    start_date: date
    end_date: date
    excludes: Optional[list[date]] = None
    range_type: PartitionType = PartitionType.DAY
    _dates: list[date] = field(init=False)
    _date_index: int = field(init=False)

    def __post_init__(self):
        """Initialize internal attributes."""
        if self.range_type not in (PartitionType.DAY, PartitionType.MONTH):
            raise ValueError(f"Unsupported partitioning type: {self.range_type}")

        match self.range_type:
            case PartitionType.DAY:
                self._dates = [
                    self.start_date + timedelta(i)
                    for i in range((self.end_date - self.start_date).days + 1)
                ]
            case PartitionType.MONTH:
                self._dates = [
                    dt.date()
                    for dt in rrule(
                        freq=MONTHLY,
                        dtstart=self.start_date.replace(day=1),
                        until=self.end_date,
                    )
                ]
                # Dates in excluded must be the first day of the month to match `dates`
                log.info("Converting excludes to first day of the month.")
                if self.excludes is not None:
                    self.excludes = [day.replace(day=1) for day in self.excludes]

    def __iter__(self):
        """Make iterator object."""
        self._date_index = 0
        return self

    def __next__(self):
        """Get the next date in the iteration."""
        if self._date_index < len(self._dates):
            _date = self._dates[self._date_index]
            self._date_index += 1
            if self._is_date_excluded(_date):
                log.info(f"Skipping excluded date: {_date}")
                return self.__next__()
            else:
                return _date
        else:
            raise StopIteration

    def _is_date_excluded(self, d: date) -> bool:
        return self.excludes is not None and d in self.excludes


def apply_backfill_date_partition_offset(
    backfill_date: date, partition_offset: Optional[timedelta]
) -> date:
    """Apply the whole days of a partition offset to a backfill date.

    Sub-day offsets like "-1h" are truncated toward zero so each backfill date still maps to its own partition.
    """
    if partition_offset:
        # We can't simply return `backfill_date + partition_offset` because that'd only take into account the
        # timedelta's days component, and timedelta normalizes negative values to negative days plus positive seconds
        # (e.g. "-1h" is normalized as `timedelta(days=-1, seconds=82800)`, which would shift the date back a whole day).
        return backfill_date + timedelta(days=int(partition_offset / timedelta(days=1)))
    return backfill_date


def _get_partition_date(
    backfill_date: date,
    partitioning_type: PartitionType,
    partition_offset: Optional[timedelta],
) -> date:
    match partitioning_type:
        case PartitionType.DAY:
            return apply_backfill_date_partition_offset(backfill_date, partition_offset)
        case PartitionType.MONTH:
            if partition_offset:
                raise NotImplementedError("Offsets unsupported for monthly partitions.")
            return backfill_date
        case _:
            raise NotImplementedError(
                f"Unsupported partitioning type {partitioning_type}."
            )


def get_backfill_partition(
    backfill_date: date,
    partition_parameter: Optional[str],
    partition_offset: Optional[timedelta],
    partitioning_type: PartitionType,
) -> Optional[str]:
    """Get the partition to write to for a particular backfill date based on query settings."""
    # If partition_parameter is explicitly set to None the target is the entire table.
    # However if there's a partition_offset, the target is still a specific partition.
    if not partition_parameter and not partition_offset:
        return None

    partition_date = _get_partition_date(
        backfill_date, partitioning_type, partition_offset
    )

    partition = None
    match partitioning_type:
        case PartitionType.DAY:
            partition = partition_date.strftime("%Y%m%d")
        case PartitionType.MONTH:
            partition = partition_date.strftime("%Y%m")

    return partition
