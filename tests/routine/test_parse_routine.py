import os
from pathlib import Path

from bigquery_etl.routine import parse_routine

TEST_DIR = Path(__file__).parent.parent


class TestParseRoutine:
    udf_dir = TEST_DIR / "data" / "test_sql" / "moz-fx-data-test-project" / "udf"

    def test_routine_instantiation(self):
        raw_routine = parse_routine.RawRoutine(
            self.udf_dir / "test_js_udf" / "udf.sql",
            "udf.test_js_udf",
            "udf",
            "moz-fx-data-test-project",
        )

        assert raw_routine.filepath == self.udf_dir / "test_js_udf" / "udf.sql"
        assert raw_routine.name == "udf.test_js_udf"
        assert raw_routine.dataset == "udf"
        assert raw_routine.project == "moz-fx-data-test-project"
        assert raw_routine.definitions == []
        assert raw_routine.tests == []
        assert raw_routine.dependencies == []
        assert raw_routine.description == "Some description"
        assert raw_routine.is_stored_procedure is False

    def test_raw_routine_from_file(self):
        result = parse_routine.RawRoutine.from_file(
            self.udf_dir / "test_bitmask_lowest_28" / "udf.sql"
        )
        assert result.name == "udf.test_bitmask_lowest_28"
        assert result.dataset == "udf"
        assert len(result.definitions) == 1
        assert (
            result.definitions[0]
            == """CREATE OR REPLACE FUNCTION udf.test_bitmask_lowest_28() AS (
  0x0FFFFFFF
);"""
        )
        assert result.tests == []
        assert result.dependencies == []

        result = parse_routine.RawRoutine.from_file(
            self.udf_dir / "test_shift_28_bits_one_day" / "udf.sql"
        )
        assert len(result.tests) == 1
        assert (
            "assert_equals(0, udf.test_shift_28_bits_one_day(1 << 27));"
            in result.tests[0]
        )
        assert result.dependencies == ["udf.test_bitmask_lowest_28"]

    def test_parse_routine(self):
        raw_routine = parse_routine.RawRoutine.from_file(
            self.udf_dir / "test_shift_28_bits_one_day" / "udf.sql"
        )
        result = parse_routine.ParsedRoutine.from_raw(raw_routine, "SELECT 'test'")
        assert result.tests_full_sql == "SELECT 'test'"
        assert result.name == "udf.test_shift_28_bits_one_day"

    def test_routine_usages_in_text(self):
        text = (
            "SELECT hist.extract(123) AS a, event_analysis.get_count_sql('{}') AS b,"
            " udf.test_bitmask_lowest_28(1)"
        )
        result = parse_routine.routine_usages_in_text(text, project=self.udf_dir.parent)
        assert "hist.extract" in result
        assert "event_analysis.get_count_sql" in result
        assert "udf.test_bitmask_lowest_28" in result

        text = ""
        assert (
            parse_routine.routine_usages_in_text(text, project=self.udf_dir.parent)
            == []
        )

        text = "SELECT mozfun.unknown.udf(1) AS a, udf.test_js_udf(1) AS b"
        result = parse_routine.routine_usages_in_text(text, project=self.udf_dir.parent)
        assert result == ["udf.test_js_udf"]

    def test_read_routine_dir(self):
        raw_routines = parse_routine.read_routine_dir(self.udf_dir)
        assert "udf.test_shift_28_bits_one_day" in raw_routines
        assert "udf.test_safe_crc32_uuid" in raw_routines
        assert "udf.test_safe_sample_id" in raw_routines
        assert "udf.test_shift_28_bits_one_day"
        assert (
            raw_routines["udf.test_shift_28_bits_one_day"].name
            == "udf.test_shift_28_bits_one_day"
        )
        assert (
            type(raw_routines["udf.test_shift_28_bits_one_day"])
            == parse_routine.RawRoutine
        )

    def test_parse_routine_dirs(self):
        parsed_routines = list(parse_routine.parse_routines(self.udf_dir.parent))
        bitmask_lowest_28 = [
            u for u in parsed_routines if u.name == "udf.test_bitmask_lowest_28"
        ][0]
        assert type(bitmask_lowest_28) == parse_routine.ParsedRoutine

        shift_28_bits_one_day = [
            u for u in parsed_routines if u.name == "udf.test_shift_28_bits_one_day"
        ][0]
        assert (
            "assert_equals(0, udf_test_shift_28_bits_one_day(1 << 27));"
            in shift_28_bits_one_day.tests_full_sql[0]
        )

    def test_accumulate_dependencies(self):
        raw_routines = parse_routine.read_routine_dir(self.udf_dir)

        result = parse_routine.accumulate_dependencies(
            [], raw_routines, "udf.test_shift_28_bits_one_day"
        )
        assert "udf.test_shift_28_bits_one_day" in result
        assert "udf.test_bitmask_lowest_28" in result

        result = parse_routine.accumulate_dependencies(
            [], raw_routines, "udf.test_bitmask_lowest_28"
        )
        assert "udf.test_bitmask_lowest_28" in result

    def test_routine_usage_definitions(self):
        raw_routines = parse_routine.read_routine_dir(self.udf_dir)

        text = "SELECT udf.test_bitmask_lowest_28(0), udf.test_safe_sample_id('')"
        result = parse_routine.routine_usage_definitions(
            text, self.udf_dir.parent, raw_routines
        )
        assert len(result) == 11
        assert (
            "CREATE OR REPLACE FUNCTION udf.test_bitmask_lowest_28()"
            + " AS (\n  0x0FFFFFFF\n);"
            in result
        )
        assert (
            "CREATE OR REPLACE FUNCTION udf.test_safe_sample_id(client_id STRING) AS"
            + " (\n  MOD(udf.test_safe_crc32_uuid(CAST(client_id AS BYTES)), 100)\n);"
            in result
        )

    def test_sub_local_routines(self):
        data_dir = TEST_DIR / "data" / "test_sql" / "moz-fx-data-test-project"
        raw_routines = parse_routine.read_routine_dir(data_dir / "udf")
        raw_routines.update(parse_routine.read_routine_dir(data_dir / "procedure"))
        raw_routine = parse_routine.RawRoutine.from_file(
            data_dir / "udf" / "test_shift_28_bits_one_day" / "udf.sql"
        ).tests[0]

        assert "CREATE TEMP FUNCTION" not in raw_routine
        assert "CREATE TEMP FUNCTION udf_test_bitmask_lowest_28" not in raw_routine
        result = parse_routine.sub_local_routines(
            raw_routine, self.udf_dir.parent, raw_routines
        )
        assert "CREATE TEMP FUNCTION udf_test_shift_28_bits_one_day" in result
        assert "CREATE TEMP FUNCTION udf_test_bitmask_lowest_28" in result

        text = "SELECT udf.test_bitmask_lowest_28(23), mozfun.hist.extract('{}')"
        result = parse_routine.sub_local_routines(
            text, self.udf_dir.parent, raw_routines
        )
        assert "CREATE TEMP FUNCTION udf_test_bitmask_lowest_28" in result

        # There is no defn for hist.extract in the `raw_routines`,
        # so we expect this to be unreplaced
        assert "hist_extract" not in result
        assert "mozfun.hist.extract" in result

        text = "CALL procedure.test_procedure(23);"
        result = parse_routine.sub_local_routines(
            text, self.udf_dir.parent, raw_routines, stored_procedure_test=True
        )
        assert (
            "CREATE OR REPLACE PROCEDURE\n  _generic_dataset_.procedure_test_procedure"
            in result
        )

    def test_routine_tests_sql(self):
        raw_routines = parse_routine.read_routine_dir(self.udf_dir)
        raw_routine = parse_routine.RawRoutine.from_file(
            self.udf_dir / "test_shift_28_bits_one_day" / "udf.sql"
        )
        result = parse_routine.routine_tests_sql(
            raw_routine, raw_routines, self.udf_dir.parent
        )[0]
        assert "CREATE TEMP FUNCTION udf_test_shift_28_bits_one_day" in result
        assert "CREATE TEMP FUNCTION udf_test_bitmask_lowest_28" in result

        raw_routine = parse_routine.RawRoutine.from_file(
            self.udf_dir / "test_bitmask_lowest_28" / "udf.sql"
        )
        result = parse_routine.routine_tests_sql(
            raw_routine, raw_routines, self.udf_dir.parent
        )
        assert result == []

    def test_udf_description(self):
        raw_routine = parse_routine.RawRoutine.from_file(
            self.udf_dir / "test_shift_28_bits_one_day" / "udf.sql"
        )
        assert (
            raw_routine.description
            == "Shift input bits one day left and drop any bits beyond 28 days."
        )

    def test_procedure(self, tmp_path):
        text = (
            "CREATE OR REPLACE PROCEDURE procedure.test_procedure(out STRING) "
            "BEGIN "
            "SET out = mozfun.json.mode_last('{}'); "
            "END "
        )
        procedure_file = (
            tmp_path / "procedure" / "test_procedure" / "stored_procedure.sql"
        )
        os.makedirs(procedure_file.parent)
        procedure_file.write_text(text)
        result = parse_routine.RawRoutine.from_file(procedure_file)
        assert result.name == "procedure.test_procedure"
        assert len(result.definitions) == 1
        assert len(result.dependencies) == 1
        assert "json.mode_last" in result.dependencies
        assert result.tests == [text.strip()]

        text = (
            "CREATE OR REPLACE PROCEDURE procedure.test_procedure(out STRING) "
            "BEGIN "
            "SET out = ''; "
            "END "
        )
        procedure_file.write_text(text)
        result = parse_routine.RawRoutine.from_file(procedure_file)
        assert result.name == "procedure.test_procedure"
        assert len(result.definitions) == 1
        assert result.dependencies == []
        assert result.tests == [text.strip()]

    def test_procedure_from_file(self):
        result = parse_routine.RawRoutine.from_file(
            self.udf_dir.parent
            / "procedure"
            / "test_procedure"
            / "stored_procedure.sql"
        )
        assert result.name == "procedure.test_procedure"
        assert result.dataset == "procedure"
        assert len(result.definitions) == 1
        assert (
            result.definitions[0]
            == """CREATE OR REPLACE PROCEDURE
  procedure.test_procedure(out STRING)
BEGIN
  SET out = mozfun.json.mode_last('{}');
END;"""
        )
        assert result.tests == [result.definitions[0].strip()]
        assert result.dependencies == ["json.mode_last"]

    def test_procedure_tests_sql(self):
        raw_procedure = parse_routine.RawRoutine.from_file(
            self.udf_dir.parent / "procedure" / "append_hello" / "stored_procedure.sql"
        )

        raw_routines = parse_routine.read_routine_dir(self.udf_dir)
        raw_routines.update(
            parse_routine.read_routine_dir(self.udf_dir.parent / "procedure")
        )

        tests = parse_routine.routine_tests_sql(
            raw_procedure, raw_routines, self.udf_dir.parent
        )
        assert (
            "CREATE OR REPLACE PROCEDURE\n  _generic_dataset_.procedure_test_procedure"
            in tests[0]
        )
        assert (
            "CREATE OR REPLACE PROCEDURE\n  _generic_dataset_.procedure_append_hello"
            in tests[0]
        )

        assert (
            "CREATE OR REPLACE PROCEDURE\n  _generic_dataset_.procedure_test_procedure"
            in tests[1]
        )
        assert (
            "CREATE OR REPLACE PROCEDURE\n  _generic_dataset_.procedure_append_hello"
            in tests[1]
        )
        assert (
            "CREATE OR REPLACE FUNCTION _generic_dataset_.udf_test_shift_28_bits_one_day"
            in tests[1]
        )

    def test_get_routines_from_dir(self):
        routines = parse_routine.get_routines_from_dir(self.udf_dir.parent)
        assert len(routines) == 7

        routines = parse_routine.get_routines_from_dir("non-existing")
        assert len(routines) == 0
