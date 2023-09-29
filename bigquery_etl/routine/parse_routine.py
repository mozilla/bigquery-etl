"""
Machinery for parsing UDFs and tests defined in .sql files.

This should eventually be refactored to a more general library for
parsing UDF dependencies in queries as well.
"""

import os
import re
from pathlib import Path
from typing import List

import attr
import sqlparse
import yaml

from bigquery_etl.config import ConfigLoader
from bigquery_etl.metadata.parse_metadata import METADATA_FILE
from bigquery_etl.util.common import render

UDF_CHAR = "[a-zA-Z0-9_]"
UDF_FILE = "udf.sql"
PROCEDURE_FILE = "stored_procedure.sql"
ROUTINE_FILE = (UDF_FILE, PROCEDURE_FILE)
TEMP_UDF_RE = re.compile(f"(?:udf|assert)_{UDF_CHAR}+")
PERSISTENT_UDF_PREFIX_RE_STR = (
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)(?:\s+IF\s+NOT\s+EXISTS)?"
)
PERSISTENT_UDF_PREFIX = re.compile(PERSISTENT_UDF_PREFIX_RE_STR, re.IGNORECASE)
PERSISTENT_UDF_RE = re.compile(
    rf"{PERSISTENT_UDF_PREFIX_RE_STR}\s+`?(?:[a-zA-Z0-9_-]*`?\.)?({UDF_CHAR}*)\.({UDF_CHAR}+)`?",
    re.IGNORECASE,
)
UDF_NAME_RE = re.compile(r"^([a-zA-Z0-9_]+\.)?[a-zA-Z][a-zA-Z0-9_]{0,255}$")
GENERIC_DATASET = "_generic_dataset_"

raw_routines = {}


def get_routines_from_dir(project_dir):
    """Return all UDFs and stored procedures in the project directory."""
    return [
        {
            "name": root.split("/")[-2] + "." + root.split("/")[-1],
            "project": root.split("/")[-3],
            "is_udf": filename == UDF_FILE,
        }
        for root, dirs, files in os.walk(project_dir)
        for filename in files
        if filename in ROUTINE_FILE
    ]


def get_routines(project):
    """Return all routines that could be referenced by the project."""
    return get_routines_from_dir(project) + get_routines_from_dir(
        Path(ConfigLoader.get("default", "sql_dir", fallback="sql")) / "mozfun"
    )


@attr.s(auto_attribs=True)
class RawRoutine:
    """Representation of the content of a single routine sql file."""

    filepath: str = attr.ib()
    name: str = attr.ib()
    dataset: str = attr.ib()
    project: str = attr.ib()
    definitions: List[str] = attr.ib([])
    tests: List[str] = attr.ib([])
    dependencies: List[str] = attr.ib([])
    description: str = attr.ib()
    is_stored_procedure: bool = attr.ib(False)

    @name.validator
    def validate_name(self, attribute, value):
        """Check that name is correctly derived."""
        filepath = Path(str(self.filepath))
        name = filepath.parent.name
        persistent_name = f"{self.dataset}.{name}"
        temp_name = f"{self.dataset}_{name}"

        if value is None or value != persistent_name:
            raise ValueError(
                f"Expected a function named {persistent_name} "
                f"or {temp_name} to be defined."
            )
        if not UDF_NAME_RE.match(name):
            raise ValueError(
                f"Invalid UDF name {name}: Must start with alpha char, "
                f"limited to chars {UDF_CHAR}, be at most 256 chars long"
            )

    @dataset.validator
    def validate_dataset(self, attribute, value):
        """Check that dataset name is valid."""
        if value != Path(self.filepath).parent.parent.name:
            raise ValueError("Invalid dataset name.")

    @project.validator
    def validate_project(self, attribute, value):
        """Check that project name is valid."""
        if value != Path(self.filepath).parent.parent.parent.name:
            raise ValueError("Invalid project name.")

    @dataset.default
    def set_default_dataset_name(self):
        """Set dataset default value to be derived from filepath."""
        return Path(str(self.filepath)).parent.parent.name

    @project.default
    def set_project_default_name(self):
        """Set project default value to be derived from filepath."""
        return Path(str(self.filepath)).parent.parent.parent.name

    @description.default
    def set_description_default(self):
        """Set description default value."""
        filepath = Path(str(self.filepath))
        metadata_file = filepath.parent / METADATA_FILE
        if metadata_file.exists():
            metadata = yaml.safe_load(metadata_file.read_text())
            if "description" in metadata:
                return metadata["description"]
        else:
            return ""

    @classmethod
    def from_file(cls, path):
        """Create a RawRoutine instance from text."""
        filepath = Path(path)
        text = render(
            filepath.name,
            template_folder=filepath.parent,
            format=False,
        )

        sql = sqlparse.format(text, strip_comments=True)
        statements = [s for s in sqlparse.split(sql) if s.strip()]

        name = filepath.parent.name
        dataset = filepath.parent.parent.name
        project = filepath.parent.parent.parent

        persistent_name_re = rf"`?{dataset}`?.`?{name}`?"
        persistent_name = f"{dataset}.{name}"
        temp_name = f"{dataset}_{name}"
        internal_name = None
        is_stored_procedure = False

        definitions = []
        tests = []

        procedure_start = -1

        for i, s in enumerate(statements):
            normalized_statement = " ".join(s.lower().split())
            if normalized_statement.startswith("create or replace function"):
                definitions.append(s)
                if re.search(persistent_name_re, normalized_statement):
                    internal_name = persistent_name

            elif normalized_statement.startswith("create temp function"):
                definitions.append(s)
                if temp_name in normalized_statement:
                    internal_name = temp_name

            elif normalized_statement.startswith("create or replace procedure"):
                is_stored_procedure = True
                definitions.append(s)
                tests.append(s)
                if re.search(persistent_name_re, normalized_statement):
                    internal_name = persistent_name

            else:
                if normalized_statement.startswith("begin"):
                    procedure_start = i

                if procedure_start == -1:
                    tests.append(s)

                if procedure_start > -1 and normalized_statement.endswith("end;"):
                    tests.append(" ".join(statements[procedure_start : i + 1]))
                    procedure_start = -1

        # get routines that could be referenced by the UDF
        routines = get_routines(project)
        dependencies = []
        for udf in routines:
            udf_re = re.compile(
                r"\b"
                + r"\.".join(f"`?{name}`?" for name in udf["name"].split("."))
                + r"\("
            )
            if udf_re.search("\n".join(definitions)):
                dependencies.append(udf["name"])

        dependencies.extend(re.findall(TEMP_UDF_RE, "\n".join(definitions)))
        dependencies = list(set(dependencies))

        if internal_name in dependencies:
            dependencies.remove(internal_name)

        return cls(
            name=internal_name,
            filepath=path,
            definitions=definitions,
            tests=tests,
            dependencies=sorted(dependencies),
            is_stored_procedure=is_stored_procedure,
        )


@attr.s(auto_attribs=True)
class ParsedRoutine(RawRoutine):
    """Parsed representation of a routine including dependent routine code."""

    tests_full_sql: List[str] = attr.ib([])

    @staticmethod
    def from_raw(raw_routine, tests_full_sql):
        """Promote a RawRoutine to a ParsedRoutine."""
        return ParsedRoutine(*attr.astuple(raw_routine), tests_full_sql)


def read_routine_dir(*project_dirs):
    """Read contents of routine dirs into dict of RawRoutine instances."""
    global raw_routines

    if not project_dirs:
        project_dirs = (ConfigLoader.get("default", "sql_dir"),)

    if project_dirs not in raw_routines:
        raw_routines[project_dirs] = {
            raw_routine.name: raw_routine
            for project_dir in project_dirs
            for root, dirs, files in os.walk(project_dir)
            if os.path.basename(root) != ConfigLoader.get("routine", "example_dir")
            for filename in files
            if filename in ROUTINE_FILE
            for raw_routine in (RawRoutine.from_file(os.path.join(root, filename)),)
        }

    return raw_routines[project_dirs]


def parse_routines(project_dir):
    """Read routine contents of the project dir into ParsedRoutine instances."""
    # collect udfs to parse
    raw_routines = read_routine_dir(
        project_dir,
        Path(ConfigLoader.get("default", "sql_dir", fallback="sql")) / "mozfun",
    )

    # prepend udf definitions to tests
    for raw_routine in raw_routines.values():
        tests_full_sql = routine_tests_sql(raw_routine, raw_routines, project_dir)
        yield ParsedRoutine.from_raw(raw_routine, tests_full_sql)


def accumulate_dependencies(deps, raw_routines, udf_name):
    """
    Accumulate a list of dependent routine names.

    Given a dict of raw_routines and a udf_name string, recurse into the
    routine's dependencies, adding the names to deps in depth-first order.
    """
    if udf_name not in raw_routines:
        return deps

    raw_routine = raw_routines[udf_name]
    for dep in raw_routine.dependencies:
        deps = accumulate_dependencies(deps, raw_routines, dep)
    if udf_name in deps:
        return deps
    else:
        return deps + [udf_name]


def routine_usages_in_text(text, project):
    """Return a list of routine names used in the provided SQL text."""
    sql = sqlparse.format(text, strip_comments=True)
    routines = get_routines(project)

    udf_usages = []

    for routine in routines:
        if routine["name"] in sql:
            udf_usages.append(routine["name"])

    # the TEMP_UDF_RE matches udf_js, remove since it's not a valid UDF
    tmp_udfs = list(filter(lambda u: u != "udf_js", TEMP_UDF_RE.findall(sql)))
    udf_usages.extend(tmp_udfs)

    return sorted(set(udf_usages))


def routine_usage_definitions(text, project, raw_routines=None):
    """Return a list of definitions of routines used in provided SQL text."""
    if raw_routines is None:
        raw_routines = read_routine_dir()
    deps = []
    for udf_usage in routine_usages_in_text(text, project):
        deps = accumulate_dependencies(deps, raw_routines, udf_usage)
    return [
        statement
        for udf_name in deps
        for statement in raw_routines[udf_name].definitions
        if statement not in text
    ]


def sub_local_routines(test, project, raw_routines=None, stored_procedure_test=False):
    """
    Transform persistent UDFs into temporary UDFs for UDF tests.

    Use generic dataset for stored procedure tests.
    """
    if raw_routines is None:
        raw_routines = read_routine_dir()

    sql = prepend_routine_usage_definitions(test, project, raw_routines)

    for name, routine in raw_routines.items():
        if name in sql:
            for defn in routine.definitions:
                match = PERSISTENT_UDF_RE.match(defn)
                dataset, name = match.group(1), match.group(2)
                replace_name = f"{dataset}_{name}"
                if stored_procedure_test:
                    replace_name = f"{GENERIC_DATASET}.{replace_name}"
                sql = re.sub(
                    rf"`?(?:`?{routine.project}`?\.)?{dataset}.{name}`?",
                    replace_name,
                    sql,
                )

    if not stored_procedure_test:
        sql = PERSISTENT_UDF_PREFIX.sub("CREATE TEMP FUNCTION", sql)
    return sql


def routine_tests_sql(raw_routine, raw_routines, project):
    """
    Create tests for testing persistent UDFs.

    Persistent UDFs need to be rewritten as temporary UDFs so that changes
    can be tested.
    """
    tests_full_sql = []
    for test in raw_routine.tests:
        test_sql = sub_local_routines(
            test, project, raw_routines, raw_routine.is_stored_procedure
        )
        tests_full_sql.append(test_sql)

    return tests_full_sql


def prepend_routine_usage_definitions(text, project, raw_routines=None):
    """Prepend definitions of UDFs used to provided SQL text."""
    statements = routine_usage_definitions(text, project, raw_routines)
    return "\n\n".join(statements + [text])
