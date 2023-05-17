from bigquery_etl.util.udf_functions import get_input, get_output, get_udf_parameters


class TestUDFFunctions:
    class TestGetInput:
        def test_get_input_single_input(self):
            assert (
                get_input(
                    "CREATE OR REPLACE FUNCTION test_dataset.test_udf(input1 INT64)"
                )
                == "input1 INT64"
            )

        def test_get_input_multiple_inputs(self):
            assert (
                get_input(
                    "CREATE OR REPLACE FUNCTION test_thing.test_udf(input1 INT64, input2 FLOAT64, length INT64)"
                )
                == "input1 INT64, input2 FLOAT64, length INT64"
            )

        def test_get_input_array_struct_input(self):
            assert (
                get_input(
                    """CREATE OR REPLACE FUNCTION glam.histogram_fill_buckets_dirichlet(input_map ARRAY<STRUCT<key STRING, value FLOAT64>>)"""
                )
                == "input_map ARRAY<STRUCT<key STRING, value FLOAT64>>"
            )

        def test_get_input_multiline_input(self):
            assert (
                get_input(
                    """CREATE OR REPLACE FUNCTION glam.histogram_fill_buckets_dirichlet(
                input_map ARRAY<STRUCT<key STRING, value FLOAT64>>,
                buckets ARRAY<STRING>,
                total_users INT64
                )"""
                )
                == "input_map ARRAY<STRUCT<key STRING, value FLOAT64>>, buckets ARRAY<STRING>, total_users INT64"
            )

        def test_get_input_multiline_input_with_udf_body(self):
            assert (
                get_input(
                    """CREATE OR REPLACE FUNCTION glam.histogram_fill_buckets_dirichlet(
                input_map ARRAY<STRUCT<key STRING, value FLOAT64>>,
                buckets ARRAY<STRING>,
                total_users INT64
                ) AS
                SELECT 1"""
                )
                == "input_map ARRAY<STRUCT<key STRING, value FLOAT64>>, buckets ARRAY<STRING>, total_users INT64"
            )

        def test_get_input_none_input(self):
            assert get_input("some other text") is None

    class TestGetOutput:
        def test_get_output_single_output(self):
            assert get_output("RETURNS BOOLEAN AS (") == "BOOLEAN"

        def test_get_output_array_struct_output(self):
            assert (
                get_output(
                    "total_users INT64)RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS ("
                )
                == "ARRAY<STRUCT<key STRING, value FLOAT64>>"
            )

        def test_get_output_with_udf_body_single_line(self):
            assert (
                get_output(
                    """CREATE OR REPLACE FUNCTION glam.histogram_fill_buckets_dirichlet(
                        input_map ARRAY<STRUCT<key STRING, value FLOAT64>>,
                        buckets ARRAY<STRING>,
                        total_users INT64
                        ) RETURNS BOOLEAN AS SELECT 1"""
                )
                == "BOOLEAN"
            )

        def test_get_output_with_udf_body_multiline(self):
            assert (
                get_output(
                    """CREATE OR REPLACE FUNCTION glam.histogram_fill_buckets_dirichlet(
                    input_map ARRAY<STRUCT<key STRING, value FLOAT64>>,
                    buckets ARRAY<STRING>,
                    total_users INT64
                    ) RETURNS BOOLEAN AS
                    SELECT 1"""
                )
                == "BOOLEAN"
            )

        def test_get_output_from_cast(self):
            assert (
                get_output(
                    """CREATE OR REPLACE FUNCTION bits28.from_string(s STRING) AS (
                IF(
                    REGEXP_CONTAINS(s, r"^[01]{1,28}$"),
                    (
                    SELECT
                        SUM(CAST(c AS INT64) << (LENGTH(s) - 1 - bit))
                    FROM
                        UNNEST(SPLIT(s, '')) AS c
                        WITH OFFSET bit
                    ),
                    ERROR(FORMAT("bits28_from_string expects a string of up to 28 0's and 1's but got: %s", s))
                )
                );"""
                )
                == "INT64"
            )

        def test_get_output_multiple_matches(self):
            assert (
                get_output(
                    """CREATE OR REPLACE FUNCTION foo.bar(s STRING) RETURNS INT64 AS (
                        SELECT CAST(s AS FLOAT64)
                    )
                    """
                )
                == "INT64"
            )

        def test_get_output_without_as(self):
            # note that the odd string formatting below is intentional and spacing affects test results
            assert (
                get_output(
                    """
                    CREATE OR REPLACE FUNCTION norm.get_windows_info(os_version STRING)
RETURNS STRUCT<name STRING, version_name STRING, version_number DECIMAL, build_number INT64>
LANGUAGE js AS r\"\"\"
  const test = "test";
  return test;
\"\"\";
                    """
                )
                == "STRUCT<name STRING, version_name STRING, version_number DECIMAL, build_number INT64> LANGUAGE js"
            )

    class TestGetUDFParameters:
        def test_get_udf_parameters_1(self):
            sql_text = """CREATE OR REPLACE FUNCTION test_dataset.test_udf(input1 INT64, input2 FLOAT64) RETURNS INT64 AS SELECT 4"""

            input_part, output_part = get_udf_parameters(sql_text)
            assert input_part == "input1 INT64, input2 FLOAT64"
            assert output_part == "INT64"

        def test_get_udf_parameters_2(self):
            sql_text = """/*

                Return a boolean indicating if any bits are set in the specified range of a bit pattern.

                The start_offset must be zero or a negative number indicating an offset from
                the rightmost bit in the pattern.

                n_bits is the number of bits to consider, counting right from the bit at start_offset.

                See detailed docs for the bits28 suite of functions:
                https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference

                */
                CREATE OR REPLACE FUNCTION bits28.active_in_range(bits INT64, start_offset INT64, n_bits INT64)
                RETURNS BOOLEAN AS (
                CASE
                    WHEN start_offset > 0
                    THEN ERROR(
                        FORMAT(
                            'start_offset must be <= 0 but was %i in call bits28_active_in_range(%i, %i, %i)',
                            start_offset,
                            bits,
                            start_offset,
                            n_bits
                        )
                        )
                    WHEN n_bits > (1 - start_offset)
                    THEN ERROR(
                        FORMAT(
                            'Reading %i bits from starting_offset %i exceeds end of bit pattern in call bits28_active_in_range(%i, %i, %i)',
                            n_bits,
                            start_offset,
                            bits,
                            start_offset,
                            n_bits
                        )
                        )
                    ELSE BIT_COUNT(bits28.range(bits, start_offset, n_bits)) > 0
                END
                );

                -- Tests
                SELECT
                assert.true(bits28.active_in_range(1 << 10, -13, 7)),
                assert.false(bits28.active_in_range(1 << 10, -6, 7)),
                assert.true(bits28.active_in_range(1, 0, 1)),
                assert.false(bits28.active_in_range(0, 0, 1));
            """

            input_part, output_part = get_udf_parameters(sql_text)
            assert input_part == "bits INT64, start_offset INT64, n_bits INT64"
            assert output_part == "BOOLEAN"

        def test_get_udf_parameters_3(self):
            sql_text = """-- udf_bucket
                CREATE OR REPLACE FUNCTION glam.histogram_bucket_from_value(buckets ARRAY<STRING>, val FLOAT64)
                RETURNS FLOAT64 AS (
                -- Bucket `value` into a histogram with min_bucket, max_bucket and num_buckets
                (
                    SELECT
                    MAX(CAST(bucket AS FLOAT64))
                    FROM
                    UNNEST(buckets) AS bucket
                    WHERE
                    val >= CAST(bucket AS FLOAT64)
                )
                );

                --Tests
                SELECT
                assert.equals(2.0, glam.histogram_bucket_from_value(["1", "2", "3"], 2.333)),
                assert.equals(NULL, glam.histogram_bucket_from_value(["1"], 0.99)),
                assert.equals(0.0, glam.histogram_bucket_from_value(["0", "1"], 0.99)),
                assert.equals(0.0, glam.histogram_bucket_from_value(["1", "0"], 0.99)),
            """

            input_part, output_part = get_udf_parameters(sql_text)
            assert input_part == "buckets ARRAY<STRING>, val FLOAT64"
            assert output_part == "FLOAT64"

        def test_get_udf_parameters_4(self):
            sql_text = """-- udf_fill_buckets
                CREATE OR REPLACE FUNCTION glam.histogram_fill_buckets_dirichlet(
                input_map ARRAY<STRUCT<key STRING, value FLOAT64>>,
                buckets ARRAY<STRING>,
                total_users INT64
                )
                RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
                -- Given a MAP `input_map`, fill in any missing keys with value `0.0` and
                -- transform values to estimate a Dirichlet distribution
                ARRAY(
                    SELECT AS STRUCT
                    key,
                    -- Dirichlet distribution density for each bucket in a histogram.
                    -- Given client level {k1: p1, k2:p2, ... , kK: pK}
                    -- where p’s are client proportions, and p1, p2, ... pK sum to 1,
                    -- k1, k2, ... , kK are the buckets, and K is the total number of buckets.
                    -- returns an array [] around a struct {k : v} such that
                    -- [{k1: (P1+1/K) / (N_reporting+1), k2:(P2+1/K) / (N_reporting+1), ...}]
                    -- where (capital) P1 is the sum of p1s, P2 for p2s, etc.:
                    --   P1 = (p1_client1 + p1_client2 + ... + p1_clientN) & 0 < P1 < N
                    -- and capital K is again the total number of buckets.
                    -- For more information, please see:
                    -- https://docs.google.com/document/d/1ipy1oFIKDvHr3R6Ku0goRjS11R1ZH1z2gygOGkSdqUg
                    SAFE_DIVIDE(
                        COALESCE(e.value, 0.0) + SAFE_DIVIDE(1, ARRAY_LENGTH(buckets)),
                        total_users + 1
                    ) AS value
                    FROM
                    UNNEST(buckets) AS key
                    LEFT JOIN
                    UNNEST(input_map) AS e
                    ON
                    key = e.key
                    ORDER BY
                    SAFE_CAST(key AS FLOAT64)
                )
                );

                SELECT
                -- fill in 1 with a value of 0
                assert.array_equals(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[
                    ("0", (1 + (1 / 3)) / 3),
                    ("1", 1 / 9),
                    ("2", (2 + (1 / 3)) / 3)
                    ],
                    glam.histogram_fill_buckets_dirichlet(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1.0), ("2", 2.0)],
                    ["0", "1", "2"],
                    2
                    )
                ),
                -- only keep values in specified in buckets
                assert.array_equals(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", (1 + 1) / 3)],
                    glam.histogram_fill_buckets_dirichlet(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1.0), ("2", 1.0)],
                    ["0"],
                    2
                    )
                ),
                -- keys may not non-integer values, so ordering is not well defined for strings
                assert.array_equals(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[
                    ("foo", (1 + (1 / 2)) / 3),
                    ("bar", (1 + (1 / 2)) / 3)
                    ],
                    glam.histogram_fill_buckets_dirichlet(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[("foo", 1.0), ("bar", 1.0)],
                    ["foo", "bar"],
                    2
                    )
                ),
                -- but ordering is guaranteed for integers/floats
                assert.array_equals(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[("2", (1 + (1 / 2)) / 3), ("11", (1 + (1 / 2)) / 3)],
                    glam.histogram_fill_buckets_dirichlet(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[("11", 1.0), ("2", 1.0)],
                    ["11", "2"],
                    2
                    )
                )
            """
            assert get_udf_parameters(sql_text) == (
                "input_map ARRAY<STRUCT<key STRING, value FLOAT64>>, buckets ARRAY<STRING>, total_users INT64",
                "ARRAY<STRUCT<key STRING, value FLOAT64>>",
            )

        def test_get_udf_parameters_5(self):
            sql_text = """-- udf_get_values
                CREATE OR REPLACE FUNCTION glam.map_from_array_offsets(
                required ARRAY<FLOAT64>,
                `values` ARRAY<FLOAT64>
                )
                RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
                (
                    SELECT
                    ARRAY_AGG(
                        STRUCT<key STRING, value FLOAT64>(
                        CAST(key AS STRING),
                        `values`[OFFSET(SAFE_CAST(key AS INT64))]
                        )
                        ORDER BY
                        key
                    )
                    FROM
                    UNNEST(required) AS key
                )
                );

                SELECT
                assert.array_equals(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 2.0), ("2", 4.0)],
                    glam.map_from_array_offsets([0.0, 2.0], [2.0, 3.0, 4.0])
                ),
                -- required ordered
                assert.array_equals(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 2.0), ("2", 4.0)],
                    glam.map_from_array_offsets([2.0, 0.0], [2.0, 3.0, 4.0])
                ),
                -- intended use-case, for approx_quantiles
                assert.array_equals(
                    ARRAY<STRUCT<key STRING, value FLOAT64>>[("25", 1.0), ("50", 2.0), ("75", 3.0)],
                    glam.map_from_array_offsets(
                    [25.0, 50.0, 75.0],
                    (
                        SELECT
                        ARRAY_AGG(CAST(y * 1.0 AS FLOAT64) ORDER BY i)
                        FROM
                        UNNEST((SELECT APPROX_QUANTILES(x, 100) FROM UNNEST([1, 2, 3]) AS x)) AS y
                        WITH OFFSET i
                    )
                    )
                )
            """

            assert get_udf_parameters(sql_text) == (
                "required ARRAY<FLOAT64>, `values` ARRAY<FLOAT64>",
                "ARRAY<STRUCT<key STRING, value FLOAT64>>",
            )

        def test_get_udf_parameters_6(self):
            sql_text = """/*

                Convert a string representing individual bits into an INT64.

                Implementation based on https://stackoverflow.com/a/51600210/1260237

                See detailed docs for the bits28 suite of functions:
                https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference

                */
                CREATE OR REPLACE FUNCTION bits28.from_string(s STRING) AS (
                IF(
                    REGEXP_CONTAINS(s, r"^[01]{1,28}$"),
                    (
                    SELECT
                        SUM(CAST(c AS INT64) << (LENGTH(s) - 1 - bit))
                    FROM
                        UNNEST(SPLIT(s, '')) AS c
                        WITH OFFSET bit
                    ),
                    ERROR(FORMAT("bits28_from_string expects a string of up to 28 0's and 1's but got: %s", s))
                )
                );

                -- Tests
                SELECT
                assert.equals(1, bits28.from_string('1')),
                assert.equals(1, bits28.from_string('01')),
                assert.equals(1, bits28.from_string('0000000000000000000000000001')),
                assert.equals(2, bits28.from_string('10')),
                assert.equals(5, bits28.from_string('101'));
            """
            assert get_udf_parameters(sql_text) == ("s STRING", "INT64")

        def test_get_udf_parameters_procedure_1(self):
            sql_text = """ CREATE OR REPLACE PROCEDURE
                event_analysis.create_count_steps_query(
                    project STRING,
                    dataset STRING,
                    events ARRAY<STRUCT<category STRING, event_name STRING>>,
                    OUT sql STRING
                )
                BEGIN
                DECLARE i INT64 DEFAULT 1;

                DECLARE event STRUCT<category STRING, event_name STRING>;

                DECLARE event_filter STRING;

                DECLARE event_filters ARRAY<STRING> DEFAULT[];

                WHILE
                    i <= ARRAY_LENGTH(events)
                DO
                    SET event = events[ORDINAL(i)];

                    SET event_filter = CONCAT(
                    '(category = "',
                    event.category,
                    '"',
                    ' AND event = "',
                    event.event_name,
                    '")'
                    );

                    SET event_filters = ARRAY_CONCAT(event_filters, [event_filter]);

                    SET i = i + 1;
                END WHILE;

                SET sql = CONCAT(
                    '\n  SELECT',
                    '\n    event_analysis.aggregate_match_strings(ARRAY_AGG(event_analysis.event_index_to_match_string(index))) AS count_regex',
                    '\n  FROM',
                    '\n    `',
                    project,
                    '`.',
                    dataset,
                    '.event_types',
                    '\n  WHERE',
                    '\n    ',
                    ARRAY_TO_STRING(event_filters, ' OR ')
                );
                END;

                -- Tests
                BEGIN
                DECLARE result_sql STRING;

                DECLARE expect STRING DEFAULT \"\"\"
                SELECT
                    event_analysis.aggregate_match_strings(ARRAY_AGG(event_analysis.event_index_to_match_string(index))) AS count_regex
                FROM
                    `moz-fx-data-shared-prod`.org_mozilla_firefox.event_types
                WHERE
                    (category = "collections" AND event = "saved") OR (category = "collections" AND event = "tabs_added")
                \"\"\";

                CALL event_analysis.create_count_steps_query(
                    'moz-fx-data-shared-prod',
                    'org_mozilla_firefox',
                    [
                    STRUCT('collections' AS category, 'saved' AS event_name),
                    STRUCT('collections' AS category, 'tabs_added' AS event_name)
                    ],
                    result_sql
                );

                SELECT
                    assert.sql_equals(expect, result_sql);
                END;

            """
            assert get_udf_parameters(sql_text) == (
                "project STRING, dataset STRING, events ARRAY<STRUCT<category STRING, event_name STRING>>",
                "sql STRING",
            )

        def test_get_udf_parameters_from_procedure_1(self):
            assert get_udf_parameters(
                "CREATE OR REPLACE PROCEDURE foo.bar(s STRING, b BOOLEAN) AS ..."
            ) == ("s STRING, b BOOLEAN", None)

        def test_get_udf_parameters_from_procedure_2(self):
            assert get_udf_parameters(
                "CREATE OR REPLACE PROCEDURE foo.bar(s STRING, b BOOLEAN, OUT sql STRING) AS ..."
            ) == ("s STRING, b BOOLEAN", "sql STRING")
