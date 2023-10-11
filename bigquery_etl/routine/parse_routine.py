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
TEMP_UDF_USAGE_RE = re.compile(rf"(?<!\.)\b(?:udf|assert)_{UDF_CHAR}+(?=\()")
PERSISTENT_UDF_PREFIX_RE_STR = (
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)(?:\s+IF\s+NOT\s+EXISTS)?"
)
PERSISTENT_UDF_PREFIX = re.compile(PERSISTENT_UDF_PREFIX_RE_STR, re.IGNORECASE)
PERSISTENT_UDF_RE = re.compile(
    rf"{PERSISTENT_UDF_PREFIX_RE_STR}\s+(?:`?([a-zA-Z0-9_-]+)`?\.)?`?({UDF_CHAR}+)`?\.`?({UDF_CHAR}+)`?",
    re.IGNORECASE,
)
UDF_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,255}$")
GENERIC_DATASET = "_generic_dataset_"

raw_routines = {}


def get_routines_from_dir(project_dir):
    """Return all UDFs and stored procedures in the project directory."""
    return [
        {
            "id": ".".join(root.split("/")[-3:]),
            "name": root.split("/")[-1],
            "dataset": root.split("/")[-2],
            "project": root.split("/")[-3],
            "is_udf": filename == UDF_FILE,
        }
        for root, dirs, files in os.walk(project_dir)
        for filename in files
        if filename in ROUTINE_FILE
    ]


def get_routines(project_dir):
    """Return all routines that could be referenced by the project."""
    return get_routines_from_dir(project_dir) + get_routines_from_dir(
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
    is_stored_procedure: bool = attr.ib()

    @name.validator
    def validate_name(self, attribute, value):
        """Check that name is valid."""
        if value != Path(self.filepath).parent.name:
            raise ValueError(
                f"Name `{value}` doesn't match filepath `{self.filepath}`."
            )
        if not UDF_NAME_RE.match(value):
            raise ValueError(
                f"Invalid UDF name `{value}`: Must start with alpha char, "
                f"limited to chars {UDF_CHAR}, be at most 256 chars long"
            )

    @dataset.validator
    def validate_dataset(self, attribute, value):
        """Check that dataset name is valid."""
        if value != Path(self.filepath).parent.parent.name:
            raise ValueError(
                f"Dataset `{value}` doesn't match filepath `{self.filepath}`."
            )

    @project.validator
    def validate_project(self, attribute, value):
        """Check that project name is valid."""
        if value != Path(self.filepath).parent.parent.parent.name:
            raise ValueError(
                f"Project `{value}` doesn't match filepath `{self.filepath}`."
            )

    @name.default
    def set_default_name(self):
        """Set name default value to be derived from filepath."""
        return Path(str(self.filepath)).parent.name

    @dataset.default
    def set_default_dataset(self):
        """Set dataset default value to be derived from filepath."""
        return Path(str(self.filepath)).parent.parent.name

    @project.default
    def set_default_project(self):
        """Set project default value to be derived from filepath."""
        return Path(str(self.filepath)).parent.parent.parent.name

    @description.default
    def set_default_description(self):
        """Set description default value."""
        filepath = Path(str(self.filepath))
        metadata_file = filepath.parent / METADATA_FILE
        if metadata_file.exists():
            metadata = yaml.safe_load(metadata_file.read_text())
            if "description" in metadata:
                return metadata["description"]
        else:
            return ""

    @is_stored_procedure.default
    def set_default_is_stored_procedure(self):
        """Set is_stored_procedure default value."""
        return Path(str(self.filepath)).name == PROCEDURE_FILE

    @property
    def id(self):
        """ID."""
        return f"{self.project}.{self.dataset}.{self.name}"

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
        project_dir = filepath.parent.parent.parent
        project = project_dir.name
        id = f"{project}.{dataset}.{name}"

        if not re.search(
            rf"{PERSISTENT_UDF_PREFIX_RE_STR}\s+(`?{project}`?\.)?`?{dataset}`?\.`?{name}`?\(",
            sql,
            re.IGNORECASE,
        ):
            raise Exception(
                f"Expected a routine named `{dataset}.{name}` to be defined."
            )

        definitions = []
        tests = []
        procedure_start = -1
        for i, s in enumerate(statements):
            normalized_statement = " ".join(s.lower().split())
            if normalized_statement.startswith("create or replace function"):
                definitions.append(s)
            elif normalized_statement.startswith("create temp function"):
                definitions.append(s)
            elif normalized_statement.startswith("create or replace procedure"):
                definitions.append(s)
                tests.append(s)
            else:
                if normalized_statement.startswith("begin"):
                    procedure_start = i
                if procedure_start == -1:
                    tests.append(s)
                if procedure_start > -1 and normalized_statement.endswith("end;"):
                    tests.append(" ".join(statements[procedure_start : i + 1]))
                    procedure_start = -1

        dependencies = routine_usages_in_text("\n".join(definitions), project_dir)
        if id in dependencies:
            dependencies.remove(id)

        return cls(
            filepath=path,
            definitions=definitions,
            tests=tests,
            dependencies=dependencies,
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
            raw_routine.id: raw_routine
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
        tests_full_sql = routine_tests_sql(raw_routine, raw_routines)
        yield ParsedRoutine.from_raw(raw_routine, tests_full_sql)


def accumulate_dependencies(deps, raw_routines, routine_id):
    """
    Accumulate a list of dependent routine IDs.

    Given a dict of raw_routines and a routine_id string, recurse into the
    routine's dependencies, adding the IDs to deps in depth-first order.
    """
    if routine_id not in raw_routines:
        return deps

    raw_routine = raw_routines[routine_id]
    for dep in raw_routine.dependencies:
        deps = accumulate_dependencies(deps, raw_routines, dep)
        if dep not in deps:
            deps = deps + [dep]
    return deps


def routine_usages_in_text(text, project_dir):
    """Return a list of routine IDs used in the provided SQL text."""
    sql = sqlparse.format(text, strip_comments=True)
    project = Path(project_dir).name
    routines = get_routines(project_dir)

    routine_usages = []

    for routine in routines:
        routine_usage_re = re.compile(
            r"(?<!\.)\b"
            rf"(`?{routine['project']}`?\.){'?' if routine['project'] == project else ''}"
            rf"`?{routine['dataset']}`?\.`?{routine['name']}`?\("
        )
        if routine_usage_re.search(sql):
            routine_usages.append(routine["id"])

    routine_usages.extend(TEMP_UDF_USAGE_RE.findall(sql))

    return sorted(set(routine_usages))


def sub_local_routines(
    test, project_dir, raw_routines=None, stored_procedure_test=False
):
    """
    Transform persistent UDFs into temporary UDFs for UDF tests.

    Use generic dataset for stored procedure tests.
    """
    if raw_routines is None:
        raw_routines = read_routine_dir()

    routine_ids = []
    for routine_id in routine_usages_in_text(test, project_dir):
        routine_ids = accumulate_dependencies(routine_ids, raw_routines, routine_id)
        # Add a routine after its dependencies so the routines get declared in the correct order.
        if routine_id not in routine_ids:
            routine_ids.append(routine_id)

    routines = [
        raw_routines[routine_id]
        for routine_id in routine_ids
        if routine_id in raw_routines
    ]

    routines_sql = "\n\n".join(
        definition
        for routine in routines
        for definition in routine.definitions
        if definition not in test
    )
    sql = routines_sql + "\n\n" + test

    for routine in routines:
        for definition in routine.definitions:
            routine_match = PERSISTENT_UDF_RE.match(definition)
            routine_project = routine_match.group(1) or routine.project
            routine_dataset = routine_match.group(2)
            routine_name = routine_match.group(3)
            replace_routine_name = re.sub(
                r"\W", "_", f"{routine_project}_{routine_dataset}_{routine_name}"
            )
            if stored_procedure_test:
                replace_routine_name = f"{GENERIC_DATASET}.{replace_routine_name}"
            sql = re.sub(
                rf"(?<!\.)\b(`?{routine_project}`?\.)?`?{routine_dataset}`?\.`?{routine_name}`?(?=\()",
                replace_routine_name,
                sql,
            )

    if not stored_procedure_test:
        sql = PERSISTENT_UDF_PREFIX.sub("CREATE TEMP FUNCTION", sql)
    return sql


def routine_tests_sql(raw_routine, raw_routines):
    """
    Create tests for testing persistent UDFs.

    Persistent UDFs need to be rewritten as temporary UDFs so that changes
    can be tested.
    """
    tests_full_sql = []
    for test in raw_routine.tests:
        project_dir = Path(raw_routine.filepath).parent.parent.parent
        test_sql = sub_local_routines(
            test, project_dir, raw_routines, raw_routine.is_stored_procedure
        )
        tests_full_sql.append(test_sql)

    return tests_full_sql
