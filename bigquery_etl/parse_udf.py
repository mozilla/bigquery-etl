"""
Machinery for parsing UDFs and tests defined in .sql files.

This should eventually be refactored to a more general library for
parsing UDF dependencies in queries as well.
"""

from dataclasses import dataclass, astuple
import re
import os
from typing import List

import sqlparse


UDF_DIRS = ("udf", "udf_js")
UDF_CHAR = "[a-zA-z0-9_]"
TEMP_UDF_RE = re.compile(f"(?:udf|assert)_{UDF_CHAR}+")
PERSISTENT_UDF_RE = re.compile(fr"((?:udf|assert){UDF_CHAR}*)\.({UDF_CHAR}+)")
UDF_NAME_RE = re.compile(r"^([a-zA-Z0-9_]+\.)?[a-zA-Z][a-zA-Z0-9_]{0,255}$")


@dataclass
class RawUdf:
    """Representation of the content of a single UDF sql file."""

    name: str
    filepath: str
    definitions: List[str]
    tests: List[str]
    dependencies: List[str]

    @staticmethod
    def from_file(filepath):
        """Read in a RawUdf from a SQL file on disk."""
        dirpath, basename = os.path.split(filepath)

        with open(filepath) as f:
            text = f.read()

        sql = sqlparse.format(text, strip_comments=True)
        statements = [s for s in sqlparse.split(sql) if s.strip()]

        prod_name = basename.replace(".sql", "")
        persistent_name = os.path.basename(dirpath) + "." + prod_name
        temp_name = os.path.basename(dirpath) + "_" + prod_name
        internal_name = None

        definitions = []

        for s in statements:
            if s.lower().startswith("create or replace function"):
                definitions.append(s)
                if persistent_name in s:
                    internal_name = persistent_name

            elif s.lower().startswith("create temp function"):
                definitions.append(s)
                if temp_name in s:
                    internal_name = temp_name

        for name in (prod_name, internal_name):
            if not UDF_NAME_RE.match(name):
                raise ValueError(
                    f"Invalid UDF name {name}: Must start with alpha char, "
                    f"limited to chars {UDF_CHAR}, be at most 256 chars long."
                )

        tests = [
            s
            for s in statements
            if not s.lower().startswith("create or replace function")
            and not s.lower().startswith("create temp function")
        ]

        # find usages of both persistent and temporary UDFs
        dependencies = re.findall(PERSISTENT_UDF_RE, "\n".join(definitions))
        dependencies = list(map(lambda t: ".".join(t), dependencies))
        dependencies.extend(re.findall(TEMP_UDF_RE, "\n".join(definitions)))

        if internal_name not in dependencies:
            raise ValueError(
                f"Expected a UDF named {internal_name} " f"to be defined in {filepath}"
            )
        dependencies.remove(internal_name)

        return RawUdf(
            internal_name,
            filepath,
            definitions,
            tests,
            # We convert the list to a set to deduplicate entries,
            # but then convert back to a list for stable order.
            sorted(set(dependencies)),
        )


@dataclass
class ParsedUdf(RawUdf):
    """Parsed representation of a UDF including dependent UDF code."""

    tests_full_sql: List[str]

    @staticmethod
    def from_raw(raw_udf, tests_full_sql):
        """Promote a RawUdf to a ParsedUdf."""
        return ParsedUdf(*astuple(raw_udf), tests_full_sql)


def read_udf_dirs(*udf_dirs):
    """Read contents of udf_dirs into dict of RawUdf instances."""
    return {
        raw_udf.name: raw_udf
        for udf_dir in (udf_dirs or UDF_DIRS)
        for root, dirs, files in os.walk(udf_dir)
        for filename in files
        if not filename.startswith(".") and filename.endswith(".sql")
        for raw_udf in (RawUdf.from_file(os.path.join(root, filename)),)
    }


def parse_udf_dirs(*udf_dirs):
    """Read contents of udf_dirs into ParsedUdf instances."""
    # collect udfs to parse
    raw_udfs = read_udf_dirs(*udf_dirs)
    # prepend udf definitions to tests
    for raw_udf in raw_udfs.values():
        tests_full_sql = udf_tests_sql(raw_udf, raw_udfs)
        yield ParsedUdf.from_raw(raw_udf, tests_full_sql)


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
    return udf_usages_in_text(text)


def udf_usages_in_text(text):
    """Return a list of UDF names used in the provided SQL text."""
    sql = sqlparse.format(text, strip_comments=True)
    udf_usages = PERSISTENT_UDF_RE.findall(sql)
    udf_usages = list(map(lambda t: ".".join(t), udf_usages))
    udf_usages.extend(TEMP_UDF_RE.findall(sql))
    return sorted(set(udf_usages))


def udf_usage_definitions(text, raw_udfs=None):
    """Return a list of definitions of UDFs used in provided SQL text."""
    if raw_udfs is None:
        raw_udfs = read_udf_dirs()
    deps = []
    for udf_usage in udf_usages_in_text(text):
        deps = accumulate_dependencies(deps, raw_udfs, udf_usage)
    return [
        statement for udf_name in deps for statement in raw_udfs[udf_name].definitions
    ]


def udf_tests_sql(raw_udf, raw_udfs):
    """
    Create tests for testing persistent UDFs. 
    Persistent UDFs need to be rewritten as temporary UDF so that changes
    can be tested.
    """
    tests_full_sql = []
    for test in raw_udf.tests:
        test_sql = prepend_udf_usage_definitions(test, raw_udfs)
        test_sql = sub_persisent_udfs_as_temp(test_sql)
        test_sql = test_sql.replace(
            "CREATE OR REPLACE FUNCTION", "CREATE TEMP FUNCTION"
        )

        tests_full_sql.append(test_sql)

    return tests_full_sql


def prepend_udf_usage_definitions(text, raw_udfs=None):
    """Prepend definitions of UDFs used to provided SQL text."""
    statements = udf_usage_definitions(text, raw_udfs)
    return "\n\n".join(statements + [text])


def sub_persisent_udfs_as_temp(text):
    """Substitute persistent UDF references with temporary UDF references."""
    return PERSISTENT_UDF_RE.sub(r"\1_\2", text)
