from pathlib import Path
from unittest import mock

from bigquery_etl.udf import parse_udf

TEST_DIR = Path(__file__).parent.parent


class TestParseUdf:
    mock_mozfun_udfs = mock.patch.object(
        parse_udf, "MOZFUN_UDFS", ["hist.range", "json.parse"]
    )

    def test_raw_udf_from_file(self):
        udf_dir = TEST_DIR / "data" / "udf"
        result = parse_udf.RawUdf.from_file(
            udf_dir / "test_bitmask_lowest_28" / "udf.sql"
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

        result = parse_udf.RawUdf.from_file(
            udf_dir / "test_shift_28_bits_one_day" / "udf.sql"
        )
        assert len(result.tests) == 1
        assert (
            "assert_equals(0, udf.test_shift_28_bits_one_day(1 << 27));"
            in result.tests[0]
        )
        assert result.dependencies == ["udf.test_bitmask_lowest_28"]

    def test_raw_udf_from_text(self):
        with self.mock_mozfun_udfs:
            text = (
                "CREATE OR REPLACE FUNCTION udf.test_udf() "
                + "AS (SELECT mozfun.json.parse('{}'))"
            )
            result = parse_udf.RawUdf.from_text(text, "udf", "test_udf", description="")
            assert result.name == "udf.test_udf"
            assert len(result.definitions) == 1
            assert len(result.dependencies) == 1
            assert "json.parse" in result.dependencies
            assert result.tests == []

            text = "CREATE OR REPLACE FUNCTION json.parse() " + "AS (SELECT 1)"
            result = parse_udf.RawUdf.from_text(text, "json", "parse", description="")
            assert result.name == "json.parse"
            assert len(result.definitions) == 1
            assert result.dependencies == []
            assert result.tests == []

    def test_parse_udf(self):
        udf_dir = TEST_DIR / "data" / "udf"
        raw_udf = parse_udf.RawUdf.from_file(
            udf_dir / "test_shift_28_bits_one_day" / "udf.sql"
        )
        result = parse_udf.ParsedUdf.from_raw(raw_udf, "SELECT 'test'")
        assert result.tests_full_sql == "SELECT 'test'"
        assert result.name == "udf.test_shift_28_bits_one_day"

    def test_udf_usages_in_text(self):
        with self.mock_mozfun_udfs:
            text = "SELECT hist.range(123) AS a, json.parse('{}') AS b, udf.test(1)"
            result = parse_udf.udf_usages_in_text(text)
            assert result == ["hist.range", "json.parse", "udf.test"]

            text = ""
            assert parse_udf.udf_usages_in_text(text) == []

            text = "SELECT mozfun.unknown.udf(1) AS a, udf_js.test(1) AS b"
            result = parse_udf.udf_usages_in_text(text)
            assert result == ["udf_js.test"]

    def test_sub_persistent_udf_names_as_temp(self):
        text = "SELECT udf.test_udf(1), udf_js.text_udf('foo_bar'), hist.range('{}')"
        result = parse_udf.sub_persistent_udf_names_as_temp(text)
        assert (
            result
            == "SELECT udf_test_udf(1), udf_js_text_udf('foo_bar'), hist.range('{}')"
        )

        text = "SELECT assert.true()"
        result = parse_udf.sub_persistent_udf_names_as_temp(text)
        assert result == "SELECT assert_true()"

    def test_read_udf_dirs(self):
        udf_dir = TEST_DIR / "data" / "udf"
        raw_udfs = parse_udf.read_udf_dirs((udf_dir))
        assert len(raw_udfs.keys()) == 4
        assert "udf.test_shift_28_bits_one_day" in raw_udfs
        assert "udf.test_safe_crc32_uuid" in raw_udfs
        assert "udf.test_safe_sample_id" in raw_udfs
        assert "udf.test_shift_28_bits_one_day"
        assert (
            raw_udfs["udf.test_shift_28_bits_one_day"].name
            == "udf.test_shift_28_bits_one_day"
        )
        assert type(raw_udfs["udf.test_shift_28_bits_one_day"]) == parse_udf.RawUdf

    def test_parse_udf_dirs(self):
        udf_dir = TEST_DIR / "data" / "udf"
        parsed_udfs = list(parse_udf.parse_udf_dirs((udf_dir)))
        assert len(parsed_udfs) == 4
        bitmask_lowest_28 = [
            u for u in parsed_udfs if u.name == "udf.test_bitmask_lowest_28"
        ][0]
        assert type(bitmask_lowest_28) == parse_udf.ParsedUdf

        shift_28_bits_one_day = [
            u for u in parsed_udfs if u.name == "udf.test_shift_28_bits_one_day"
        ][0]
        assert (
            "assert_equals(0, udf_test_shift_28_bits_one_day(1 << 27));"
            in shift_28_bits_one_day.tests_full_sql[0]
        )

    def test_accumulate_dependencies(self):
        udf_dir = TEST_DIR / "data" / "udf"
        raw_udfs = parse_udf.read_udf_dirs((udf_dir))

        result = parse_udf.accumulate_dependencies(
            [], raw_udfs, "udf.test_shift_28_bits_one_day"
        )
        assert "udf.test_shift_28_bits_one_day" in result
        assert "udf.test_bitmask_lowest_28" in result

        result = parse_udf.accumulate_dependencies(
            [], raw_udfs, "udf.test_bitmask_lowest_28"
        )
        assert "udf.test_bitmask_lowest_28" in result

    def test_udf_usage_definitions(self):
        udf_dir = TEST_DIR / "data" / "udf"
        raw_udfs = parse_udf.read_udf_dirs((udf_dir))

        text = "SELECT udf.test_bitmask_lowest_28(0), udf.test_safe_sample_id('')"
        result = parse_udf.udf_usage_definitions(text, raw_udfs)
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

    def test_persistent_udf_as_temp(self):
        udf_dir = TEST_DIR / "data" / "udf"
        raw_udfs = parse_udf.read_udf_dirs((udf_dir))
        raw_udf = parse_udf.RawUdf.from_file(
            udf_dir / "test_shift_28_bits_one_day" / "udf.sql"
        ).tests[0]

        assert "CREATE TEMP FUNCTION" not in raw_udf
        assert "CREATE TEMP FUNCTION udf_test_bitmask_lowest_28" not in raw_udf
        result = parse_udf.persistent_udf_as_temp(raw_udf, raw_udfs)
        assert "CREATE TEMP FUNCTION udf_test_shift_28_bits_one_day" in result
        assert "CREATE TEMP FUNCTION udf_test_bitmask_lowest_28" in result

        text = "SELECT udf.test_bitmask_lowest_28(23), mozfun.hist.range('{}')"
        result = parse_udf.persistent_udf_as_temp(text, raw_udfs)
        assert "CREATE TEMP FUNCTION udf_test_bitmask_lowest_28" in result
        assert "hist.range" in result
        assert "mozfun.hist.range" not in result

    def test_udf_tests_sql(self):
        udf_dir = TEST_DIR / "data" / "udf"
        raw_udfs = parse_udf.read_udf_dirs((udf_dir))
        raw_udf = parse_udf.RawUdf.from_file(
            udf_dir / "test_shift_28_bits_one_day" / "udf.sql"
        )
        result = parse_udf.udf_tests_sql(raw_udf, raw_udfs)[0]
        assert "CREATE TEMP FUNCTION udf_test_shift_28_bits_one_day" in result
        assert "CREATE TEMP FUNCTION udf_test_bitmask_lowest_28" in result

        raw_udf = parse_udf.RawUdf.from_file(
            udf_dir / "test_bitmask_lowest_28" / "udf.sql"
        )
        result = parse_udf.udf_tests_sql(raw_udf, raw_udfs)
        assert result == []
