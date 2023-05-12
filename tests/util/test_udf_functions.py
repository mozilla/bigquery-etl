from bigquery_etl.util import udf_functions


class TestUDFFunctions:
    def test_input_parameters(self):
        header1 = "CREATE OR REPLACE FUNCTION test_dataset.test_udf(input1 INT64, input2 INT64)"
        header2 = """CREATE OR REPLACE FUNCTION test_dataset.test_udf(
            input1 INT64, input2 INT64)"""
        header3 = """CREATE OR REPLACE FUNCTION test_dataset.test_udf(
                     input1 INT64,
                     input2 INT64)"""
        header4 = """CREATE OR REPLACE FUNCTION test_dataset.test_udf(
                             input1 INT64,
                             input2 INT64) AS ("""
        header5 = """CREATE OR REPLACE FUNCTION test_dataset.test_udf(
                                     input1 INT64,
                                     input2 INT64
                                     ) AS ("""

        assert udf_functions.get_parameters(header1) == [
            "input1 INT64, input2 INT64",
            "",
        ], "Test 1"
        assert udf_functions.get_parameters(header2) == [
            "input1 INT64, input2 INT64",
            "",
        ], "test 2"
        assert udf_functions.get_parameters(header3) == [
            "input1 INT64, input2 INT64",
            "",
        ], "test 3"
        assert udf_functions.get_parameters(header4) == [
            "input1 INT64, input2 INT64",
            "",
        ], "test 4"
        assert udf_functions.get_parameters(header5) == [
            "input1 INT64, input2 INT64",
            "",
        ], "test 5"
