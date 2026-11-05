import types

import pytest

from bigquery_etl.config import ConfigLoader
from bigquery_etl.pytest_plugin import routine as routine_plugin
from bigquery_etl.routine import parse_routine

TEST_PROJECTS = ["sql/mozfun", "sql/moz-fx-data-shared-prod", "sql/test-project"]


def write_routine(base, path, sql):
    routine_file = base / path / "udf.sql"
    routine_file.parent.mkdir(parents=True)
    routine_file.write_text(sql)


@pytest.fixture
def routine_tree(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    get = ConfigLoader.get

    def fake_get(*args, fallback=None):
        if args == ("routine", "test_projects"):
            return TEST_PROJECTS
        if args == ("default", "sql_dir"):
            return "sql"
        return get(*args, fallback=fallback)

    monkeypatch.setattr(ConfigLoader, "get", fake_get)

    write_routine(
        tmp_path,
        "sql/mozfun/hist/add_one",
        "CREATE OR REPLACE FUNCTION hist.add_one(x INT64) AS (x + 1);\n"
        "SELECT hist.add_one(1);\n",
    )
    write_routine(
        tmp_path,
        "sql/moz-fx-data-shared-prod/udf/add_two",
        "CREATE OR REPLACE FUNCTION udf.add_two(x INT64) AS "
        "(mozfun.hist.add_one(mozfun.hist.add_one(x)));\n"
        "SELECT udf.add_two(1);\n",
    )
    # shares its name with the shared-prod routine, which takes precedence, so it isn't indexed
    write_routine(
        tmp_path,
        "sql/test-project/udf/add_two",
        "CREATE OR REPLACE FUNCTION udf.add_two(x INT64) AS (x + 2);\n"
        "SELECT udf.add_two(1);\n",
    )
    return tmp_path


class TestRoutineIndex:
    def test_parse_inlines_dependencies(self, routine_tree):
        index = routine_plugin._RoutineIndex()
        routine = index.parse("sql/moz-fx-data-shared-prod/udf/add_two/udf.sql")

        assert routine.name == "udf.add_two"
        (test_sql,) = routine.tests_full_sql
        assert "CREATE TEMP FUNCTION hist_add_one" in test_sql
        assert "CREATE TEMP FUNCTION udf_add_two" in test_sql
        assert "SELECT udf_add_two(1)" in test_sql

    def test_last_test_project_wins(self, routine_tree):
        index = routine_plugin._RoutineIndex()
        project, _, _ = index.routines["sql/mozfun/hist/add_one/udf.sql"]
        assert project == "sql/test-project"

    def test_unknown_routine(self, routine_tree):
        index = routine_plugin._RoutineIndex()
        with pytest.raises(KeyError, match="not in any of the routine test projects"):
            index.parse("sql/other-project/udf/missing/udf.sql")
        with pytest.raises(KeyError, match="takes precedence"):
            index.parse("sql/test-project/udf/add_two/udf.sql")

    def test_listing_cache_is_only_active_inside_the_index(self, routine_tree):
        index = routine_plugin._RoutineIndex()
        index.parse("sql/moz-fx-data-shared-prod/udf/add_two/udf.sql")
        assert parse_routine._routine_listing_cache.get() is None

        # a routine written after the index was built is visible to other code
        write_routine(
            routine_tree,
            "sql/moz-fx-data-shared-prod/udf/add_three",
            "CREATE OR REPLACE FUNCTION udf.add_three(x INT64) AS (x + 3);\n",
        )
        routine_names = {
            r["name"] for r in parse_routine.get_routines("sql/test-project")
        }
        assert "udf.add_three" in routine_names

    def test_failed_index_build_is_not_retried(self, routine_tree, monkeypatch):
        # the function name doesn't match the routine dir, so parsing fails
        write_routine(
            routine_tree,
            "sql/moz-fx-data-shared-prod/udf/bad_name",
            "CREATE OR REPLACE FUNCTION udf.other_name() AS (1);\n",
        )
        builds = []

        class CountingIndex(routine_plugin._RoutineIndex):
            def __init__(self):
                builds.append(1)
                super().__init__()

        monkeypatch.setattr(routine_plugin, "_RoutineIndex", CountingIndex)
        session = types.SimpleNamespace(stash=pytest.Stash())

        with pytest.raises(ValueError, match="udf.bad_name"):
            routine_plugin._routine_index(session)
        with pytest.raises(RuntimeError, match="index failed"):
            routine_plugin._routine_index(session)
        assert len(builds) == 1
