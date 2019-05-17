#!/usr/bin/env python3
"""Longitudinal query generator."""
import argparse
import datetime
import sys
import textwrap


def parse_date(date_str):
    """Read dates in YYYMMDD format."""
    return datetime.datetime.strptime(date_str, "%Y%m%d")


def comma_separated(names):
    """Parse a comma-separated list of names."""
    return [x.strip() for x in names.split(",")]


p = argparse.ArgumentParser()
p.add_argument("--tablename", type=str, help="Table to pull data from", required=True)
p.add_argument(
    "--from",
    type=parse_date,
    help="From submission date. Defaults to 6 months before `to`.",
)
p.add_argument("--to", type=parse_date, help="To submission date", required=True)
p.add_argument(
    "--submission-date-col",
    type=str,
    help="Name of the submission date column. Defaults to submission_date_s3",
    default="submission_date_s3",
)
p.add_argument(
    "--select",
    type=str,
    help="Select statement to retrieve data with; e.g. "
    '"substr(subsession_start_date, 0, 10) as activity_date". If none given, '
    "defaults to all input columns",
    default="*",
)
p.add_argument(
    "--where",
    type=str,
    help="Where SQL clause, filtering input data. E.g. "
    "'normalized_channel = \"nightly\"'",
)
p.add_argument(
    "--grouping-column",
    type=str,
    help="Column to group data by. Defaults to client_id",
    default="client_id",
)
p.add_argument(
    "--ordering-columns",
    type=comma_separated,
    help="Columns to order the arrays by (comma separated). "
    "Defaults to submission-date-col",
)
p.add_argument(
    "--max-array-length",
    type=int,
    help="Max length of any groups history. Defaults to no limit. "
    "Negatives are ignored",
)


def six_months_before(d):
    """Calculate a date six months before the given date."""
    m = d.month - 6
    y = d.year
    if m < 0:
        m += 12
        y -= 1
    return d.replace(month=m, year=y)


def generate_sql(opts):
    """Create a BigQuery SQL query for collecting a longitudinal dataset."""
    opts.update(
        {
            "from": datetime.datetime.strftime(
                opts["from"] or six_months_before(opts["to"]), "'%Y-%m-%d'"
            ),
            "to": datetime.datetime.strftime(opts["to"], "'%Y-%m-%d'"),
            "where": "\n{}AND {}".format(" " * 10, opts["where"])
            if opts["where"]
            else "",
            "ordering_columns": ", ".join(
                opts["ordering_columns"] or [opts["submission_date_col"]]
            ),
            "limit": opts["max_array_length"] or "UNBOUNDED",
        }
    )
    if opts["grouping_column"] in opts["ordering_columns"]:
        raise ValueError(
            "{!r} can't be used as both a grouping and ordering column".format(
                opts["grouping_column"], opts["ordering_columns"]
            )
        )

    return textwrap.dedent(
        """
      WITH aggregated AS (
        SELECT
         ROW_NUMBER() OVER (PARTITION BY {grouping_column} ORDER BY {ordering_columns}) AS _n,
         {grouping_column},
         ARRAY_AGG(STRUCT({select})) OVER (PARTITION BY {grouping_column} ORDER BY {ordering_columns} ROWS BETWEEN CURRENT ROW AND {limit} FOLLOWING) AS aggcol
        FROM
          {tablename}
        WHERE
          {submission_date_col} > {from}
          AND {submission_date_col} <= {to}{where})
      SELECT
        * except (_n)
      FROM
        aggregated
      WHERE
        _n = 1
        """.format(  # noqa: E501
            **opts
        )
    )


def main(argv, out=print):
    """Print a longitudinal query to stdout."""
    opts = p.parse_args(argv[1:])
    out(generate_sql(vars(opts)))


if __name__ == "__main__":
    main(sys.argv)
