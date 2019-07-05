"""
Machinery for parsing UDFs and tests defined in .sql files.

This should eventually be refactored to a more general library for
parsing UDF dependencies in queries as well.
"""

from dataclasses import dataclass, astuple
import re
import os
from typing import List, Set

import sqlparse


UDF_RE = re.compile(r"udf_[a-zA-z0-9_]+")


@dataclass
class RawUdf:
    """Representation of the content of a single UDF sql file."""

    name: str
    filepath: str
    definitions: List[str]
    tests: List[str]
    dependencies: Set[str]

    @staticmethod
    def from_file(filepath):
        """Read in a RawUdf from a SQL file on disk."""
        name = os.path.basename(filepath).replace(".sql", "")
        with open(filepath) as f:
            text = f.read()
        sql = sqlparse.format(text, strip_comments=True)
        statements = [s for s in sqlparse.split(sql) if s.strip()]
        definitions = [
            s for s in statements if s.lower().startswith("create temp function")
        ]
        tests = [
            s for s in statements if not s.lower().startswith("create temp function")
        ]
        dependencies = re.findall(UDF_RE, "\n".join(definitions))
        dependencies.remove(name)
        return RawUdf(name, filepath, definitions, tests, set(dependencies))


@dataclass
class ParsedUdf(RawUdf):
    """Parsed representation of a UDF including dependent UDF code."""

    full_sql: str

    @staticmethod
    def from_raw(raw_udf, full_sql):
        """Promote a RawUdf to a ParsedUdf."""
        return ParsedUdf(*astuple(raw_udf), full_sql)


def read_udf_dir(d):
    """Read contents of d into RawUdf instances."""
    for root, dirs, files in os.walk(d):
        for filename in files:
            if not filename.startswith("."):
                yield RawUdf.from_file(os.path.join(root, filename))


def parse_udf_dir(d):
    """Read contents of d into ParsedUdf instances."""
    raw_udfs = {x.name: x for x in read_udf_dir(d)}
    for raw_udf in raw_udfs.values():
        deps = accumulate_dependencies([], raw_udfs, raw_udf.name)
        definitions = []
        for dep in deps:
            definitions += raw_udfs[dep].definitions
        full_sql = "\n".join(definitions)
        yield ParsedUdf.from_raw(raw_udf, full_sql)


def accumulate_dependencies(deps, raw_udfs, udf_name):
    """
    Accumulate a list of dependent UDF names.

    Given a dict of raw_udfs and a udf_name string, recurse into the
    UDF's dependencies, adding the names to deps in depth-first order.
    """
    if udf_name not in raw_udfs:
        return deps

    raw_udf = raw_udfs[udf_name]
    for dep in raw_udf.dependencies:
        deps = accumulate_dependencies(deps, raw_udfs, dep)
    if udf_name in deps:
        return deps
    else:
        return deps + [udf_name]


def udf_usages_in_file(filepath):
    """Return a list of UDF names used in the provided SQL file."""
    with open(filepath) as f:
        text = f.read()
    sql = sqlparse.format(text, strip_comments=True)
    udf_usages = re.findall(UDF_RE, sql)
    return sorted(list(set(udf_usages)))
